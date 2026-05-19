#!/usr/bin/env python3
"""
MySQL backup: dump → zip → S3-compatible upload → local cleanup → remote retention.
Intended for Ubuntu + cron. Configure via .env in the script directory or CWD.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    from dotenv import load_dotenv
except ImportError as exc:
    print(
        "Missing dependencies. Install with: pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


REQUIRED_KEYS = [
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
    "S3_ENDPOINT",
    "S3_REGION",
    "S3_BUCKET_NAME",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "BACKUP_RETENTION_DAYS",
    "LOG_FILE",
    "S3_PREFIX",
    "MIN_TMP_SPACE_MB",
]


def script_dir() -> Path:
    return Path(__file__).resolve().parent


def load_env() -> None:
    load_dotenv(script_dir() / ".env")
    load_dotenv()  # CWD .env if present


def getenv_strip(key: str, default: str | None = None) -> str | None:
    val = os.environ.get(key, default)
    if val is None:
        return None
    return val.strip()


def require_env() -> dict[str, str]:
    missing = [k for k in REQUIRED_KEYS if not getenv_strip(k)]
    if missing:
        print(
            f"Missing required environment variables: {', '.join(missing)}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return {k: getenv_strip(k) or "" for k in REQUIRED_KEYS}


def parse_min_tmp_space_mb(raw: str) -> int:
    try:
        mb = int(raw)
    except ValueError as exc:
        raise ValueError(f"MIN_TMP_SPACE_MB must be a non-negative integer, got {raw!r}") from exc
    if mb < 0:
        raise ValueError("MIN_TMP_SPACE_MB must be >= 0")
    return mb


def assert_tmp_disk_space(temp_root: Path, min_mb: int) -> None:
    """Require at least min_mb mebibytes free on the filesystem hosting temp_root."""
    try:
        usage = shutil.disk_usage(temp_root)
    except OSError as exc:
        logging.error(
            "Could not read disk usage for temporary directory %s: %s",
            temp_root,
            exc,
        )
        raise RuntimeError("Disk space check failed") from exc

    free_bytes = usage.free
    free_mb = free_bytes / (1024 * 1024)
    total_bytes = usage.total
    total_mb = total_bytes / (1024 * 1024)
    used_bytes = usage.used
    used_mb = used_bytes / (1024 * 1024)

    logging.info(
        "Disk space for %s: %.2f MiB free of %.2f MiB total (%.2f MiB used); "
        "MIN_TMP_SPACE_MB=%s",
        temp_root,
        free_mb,
        total_mb,
        used_mb,
        min_mb,
    )

    required_bytes = min_mb * 1024 * 1024
    if free_bytes < required_bytes:
        logging.error(
            "Insufficient disk space on %s: %.2f MiB free, need at least %s MiB "
            "(MIN_TMP_SPACE_MB)",
            temp_root,
            free_mb,
            min_mb,
        )
        raise RuntimeError("Insufficient temporary disk space for backup")


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def mysqldump_sql(
    env: dict[str, str],
    out_sql: Path,
) -> None:
    port = str(int(env["DB_PORT"]))
    # Avoid password on CLI: short-lived defaults file
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".cnf",
        delete=False,
        encoding="utf-8",
    ) as cnf:
        cnf.write("[client]\n")
        cnf.write(f"user={env['DB_USER']}\n")
        cnf.write(f"password={env['DB_PASSWORD']}\n")
        cnf.write(f"host={env['DB_HOST']}\n")
        cnf.write(f"port={port}\n")
        cnf_path = Path(cnf.name)
    try:
        os.chmod(cnf_path, 0o600)
        extra = getenv_strip("MYSQLDUMP_EXTRA_ARGS", "")
        cmd = [
            "mysqldump",
            f"--defaults-extra-file={cnf_path}",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--events",
            "--databases",
            env["DB_NAME"],
        ]
        if extra:
            cmd.extend(extra.split())
        logging.info("Starting mysqldump for database %s", env["DB_NAME"])
        with open(out_sql, "wb") as dump_out:
            proc = subprocess.run(
                cmd,
                stdout=dump_out,
                stderr=subprocess.PIPE,
                check=False,
            )
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"mysqldump failed (exit {proc.returncode}): {err}")
    finally:
        try:
            cnf_path.unlink(missing_ok=True)
        except OSError as exc:
            logging.warning("Could not remove temporary mysqldump config: %s", exc)


def zip_sql(sql_path: Path, zip_path: Path) -> None:
    logging.info("Zipping %s -> %s", sql_path.name, zip_path.name)
    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as zf:
        zf.write(sql_path, arcname=sql_path.name)


def s3_client(env: dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=env["S3_ENDPOINT"],
        region_name=env["S3_REGION"],
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
    )


def upload_zip(env: dict[str, str], zip_path: Path, object_key: str) -> None:
    client = s3_client(env)
    size = zip_path.stat().st_size
    logging.info(
        "Uploading to s3://%s/%s (compressed size: %s bytes)",
        env["S3_BUCKET_NAME"],
        object_key,
        size,
    )
    client.upload_file(
        str(zip_path),
        env["S3_BUCKET_NAME"],
        object_key,
    )
    logging.info("Upload finished successfully")


def _validate_retention_days(days_raw: str) -> int:
    try:
        days = int(days_raw)
    except ValueError as exc:
        logging.error("BACKUP_RETENTION_DAYS must be an integer: %s", days_raw)
        raise SystemExit(3) from exc
    if days < 0:
        logging.error("BACKUP_RETENTION_DAYS must be >= 0")
        raise SystemExit(3)
    return days


def apply_retention(env: dict[str, str]) -> None:
    days = _validate_retention_days(env["BACKUP_RETENTION_DAYS"])

    prefix = env["S3_PREFIX"].replace("\\", "/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    client = s3_client(env)
    db_name = env["DB_NAME"]
    suffix = ".sql.zip"
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")

    logging.info(
        "Retention: deleting objects under prefix %r older than %s days (before %s UTC)",
        prefix,
        days,
        cutoff.isoformat(),
    )

    try:
        for page in paginator.paginate(Bucket=env["S3_BUCKET_NAME"], Prefix=prefix):
            for obj in page.get("Contents") or []:
                key = obj["Key"]
                base = Path(key).name
                if not (base.startswith(f"{db_name}_") and base.endswith(suffix)):
                    continue
                lm = obj["LastModified"]
                if lm.tzinfo is None:
                    lm = lm.replace(tzinfo=timezone.utc)
                if lm < cutoff:
                    logging.info("Deleting expired backup object: %s", key)
                    client.delete_object(Bucket=env["S3_BUCKET_NAME"], Key=key)
                    deleted += 1
    except (ClientError, BotoCoreError) as exc:
        logging.error("Retention cleanup failed: %s", exc)
        raise

    logging.info("Retention cleanup done; removed %s object(s)", deleted)


def main() -> int:
    load_env()
    env = require_env()

    log_file = Path(env["LOG_FILE"])
    if not log_file.is_absolute():
        log_file = script_dir() / log_file

    setup_logging(log_file)
    temp_root = Path(getenv_strip("TEMP_DIR") or tempfile.gettempdir())
    temp_root.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H:%M:%S")
    base_name = f"{env['DB_NAME']}_{stamp}.sql"
    sql_path = temp_root / base_name
    zip_path = temp_root / f"{base_name}.zip"

    prefix = env["S3_PREFIX"].replace("\\", "/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    object_key = f"{prefix}{zip_path.name}"

    try:
        min_tmp_mb = parse_min_tmp_space_mb(env["MIN_TMP_SPACE_MB"])
        assert_tmp_disk_space(temp_root, min_tmp_mb)

        mysqldump_sql(env, sql_path)
        sql_size = sql_path.stat().st_size
        logging.info("Dump written: %s (size: %s bytes)", sql_path, sql_size)

        zip_sql(sql_path, zip_path)
        zip_size = zip_path.stat().st_size
        logging.info("Zip created (size: %s bytes)", zip_size)

        upload_zip(env, zip_path, object_key)
        apply_retention(env)
        logging.info("Backup completed successfully")
        return 0
    except Exception:
        logging.exception("Backup failed")
        return 1
    finally:
        for p in (sql_path, zip_path):
            try:
                if p.exists():
                    p.unlink()
                    logging.info("Removed temporary file: %s", p)
            except OSError as exc:
                logging.warning("Could not remove temporary file %s: %s", p, exc)


if __name__ == "__main__":
    sys.exit(main())
