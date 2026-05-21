#!/usr/bin/env python3
"""
MySQL backup: dump → zip → S3-compatible upload → local cleanup → remote retention.
Intended for Ubuntu + cron. Configure via .env in the script directory or CWD.
"""

from __future__ import annotations

import logging
import os
import shutil
import smtplib
import subprocess
import sys
import tempfile
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

EMAIL_ENV_KEYS = [
    "EMAIL_TO",
    "SMTP_HOST",
    "SMTP_PORT",
    "EMAIL_FROM",
]

MAX_LOG_EMAIL_BYTES = 512 * 1024


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


class RunLogHandler(logging.Handler):
    """Buffers formatted log lines for the current script execution (email body)."""

    def __init__(self, formatter: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(formatter)
        self._lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._lines.append(self.format(record))
        except Exception:
            self.handleError(record)

    def get_text(self, max_bytes: int = MAX_LOG_EMAIL_BYTES) -> tuple[str, bool]:
        text = "\n".join(self._lines)
        if not text:
            return "(no log output for this run)", False
        encoded = text.encode("utf-8")
        if len(encoded) <= max_bytes:
            return text, False
        tail = encoded[-max_bytes:].decode("utf-8", errors="replace")
        return (
            f"... (log truncated, showing last {max_bytes} bytes)\n\n{tail}",
            True,
        )


def setup_logging(log_path: Path) -> RunLogHandler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    run_handler = RunLogHandler(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
    root.addHandler(run_handler)
    return run_handler


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


def _parse_email_recipients(raw: str) -> list[str]:
    return [addr.strip() for addr in raw.replace(";", ",").split(",") if addr.strip()]


def _parse_smtp_port(raw: str) -> int:
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError(f"SMTP_PORT must be an integer, got {raw!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"SMTP_PORT must be between 1 and 65535, got {port}")
    return port


def _smtp_use_tls(port: int) -> bool:
    raw = getenv_strip("SMTP_USE_TLS")
    if raw is None:
        return port == 587
    return raw.lower() in ("1", "true", "yes", "on")


def _flush_log_handlers() -> None:
    for handler in logging.root.handlers:
        handler.flush()


def send_backup_email(
    env: dict[str, str],
    run_log: RunLogHandler,
    success: bool,
) -> None:
    to_raw = getenv_strip("EMAIL_TO")
    if not to_raw:
        return

    missing = [k for k in EMAIL_ENV_KEYS if not getenv_strip(k)]
    if missing:
        logging.error(
            "Email report status: not sent — missing variables %s (recipient: %s)",
            ", ".join(missing),
            to_raw,
        )
        return

    recipients = _parse_email_recipients(to_raw)
    recipients_display = ", ".join(recipients)
    if not recipients:
        logging.error(
            "Email report status: not sent — invalid EMAIL_TO (recipient: %s)",
            to_raw,
        )
        return

    smtp_host = getenv_strip("SMTP_HOST") or ""
    smtp_port = _parse_smtp_port(getenv_strip("SMTP_PORT") or "")
    from_addr = getenv_strip("EMAIL_FROM") or ""
    smtp_user = getenv_strip("SMTP_USER")
    smtp_password = getenv_strip("SMTP_PASSWORD")

    _flush_log_handlers()
    log_text, truncated = run_log.get_text()

    status = "SUCCESS" if success else "FAILED"
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[MySQL backup] {env['DB_NAME']} - {status} ({stamp})"

    summary_lines = [
        f"Database backup {status.lower()}.",
        f"Database: {env['DB_NAME']}",
        f"Host: {env['DB_HOST']}",
        f"Time: {stamp}",
    ]
    if truncated:
        summary_lines.append(
            f"Run log truncated in email (exceeded {MAX_LOG_EMAIL_BYTES} bytes)."
        )
    summary = "\n".join(summary_lines)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(summary, "plain", "utf-8"))
    msg.attach(
        MIMEText(
            f"--- current run log ---\n\n{log_text}",
            "plain",
            "utf-8",
        )
    )

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as smtp:
            if _smtp_use_tls(smtp_port):
                smtp.starttls()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.sendmail(from_addr, recipients, msg.as_string())
    except (OSError, smtplib.SMTPException) as exc:
        logging.error(
            "Email report status: failed — could not send to %s: %s",
            recipients_display,
            exc,
        )
        raise

    logging.info(
        "Email report status: sent — delivered to %s",
        recipients_display,
    )


def main() -> int:
    load_env()
    env = require_env()

    log_file = Path(env["LOG_FILE"])
    if not log_file.is_absolute():
        log_file = script_dir() / log_file

    run_log = setup_logging(log_file)
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

    exit_code = 0
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
    except Exception:
        logging.exception("Backup failed")
        exit_code = 1
    finally:
        for p in (sql_path, zip_path):
            try:
                if p.exists():
                    p.unlink()
                    logging.info("Removed temporary file: %s", p)
            except OSError as exc:
                logging.warning("Could not remove temporary file %s: %s", p, exc)

        if getenv_strip("EMAIL_TO"):
            try:
                send_backup_email(env, run_log, exit_code == 0)
            except Exception:
                pass  # send status already logged to backup.log

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
