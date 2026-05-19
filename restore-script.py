#!/usr/bin/env python3
"""
Interactive restore: list S3 backups (newest first), pick one, confirm, then
replace the target database with the backup contents.

Uses the same .env layout as backup-script.py (DB_* and S3_*). Restore activity
is logged to restore.log by default (override with RESTORE_LOG_FILE). Expects
dumps produced with mysqldump --databases (CREATE DATABASE + schema + routines, etc.).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
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


# Subset of backup .env: no retention / backup-only knobs required here
RESTORE_REQUIRED_KEYS = [
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
    "S3_PREFIX",
]


def script_dir() -> Path:
    return Path(__file__).resolve().parent


def load_env() -> None:
    load_dotenv(script_dir() / ".env")
    load_dotenv()


def getenv_strip(key: str, default: str | None = None) -> str | None:
    val = os.environ.get(key, default)
    if val is None:
        return None
    return val.strip()


def require_restore_env() -> dict[str, str]:
    missing = [k for k in RESTORE_REQUIRED_KEYS if not getenv_strip(k)]
    if missing:
        print(
            f"Missing required environment variables: {', '.join(missing)}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return {k: getenv_strip(k) or "" for k in RESTORE_REQUIRED_KEYS}


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


def s3_client(env: dict[str, str]):
    return boto3.client(
        "s3",
        endpoint_url=env["S3_ENDPOINT"],
        region_name=env["S3_REGION"],
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
    )


def normalized_s3_prefix(env: dict[str, str]) -> str:
    prefix = env["S3_PREFIX"].replace("\\", "/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def list_backups_newest_first(env: dict[str, str]) -> list[dict]:
    """Objects matching backup naming: {DB_NAME}_*.sql.zip under S3_PREFIX."""
    prefix = normalized_s3_prefix(env)
    db_name = env["DB_NAME"]
    suffix = ".sql.zip"
    client = s3_client(env)
    paginator = client.get_paginator("list_objects_v2")
    rows: list[dict] = []

    for page in paginator.paginate(Bucket=env["S3_BUCKET_NAME"], Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            base = Path(key).name
            if not (base.startswith(f"{db_name}_") and base.endswith(suffix)):
                continue
            lm = obj["LastModified"]
            if lm.tzinfo is None:
                lm = lm.replace(tzinfo=timezone.utc)
            rows.append(
                {
                    "key": key,
                    "last_modified": lm,
                    "size": int(obj.get("Size") or 0),
                }
            )

    rows.sort(key=lambda r: r["last_modified"], reverse=True)
    return rows


def format_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KiB"
    return f"{n / (1024 * 1024):.1f} MiB"


def write_mysql_defaults(env: dict[str, str]) -> Path:
    port = str(int(env["DB_PORT"]))
    cnf = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".cnf",
        delete=False,
        encoding="utf-8",
    )
    try:
        cnf.write("[client]\n")
        cnf.write(f"user={env['DB_USER']}\n")
        cnf.write(f"password={env['DB_PASSWORD']}\n")
        cnf.write(f"host={env['DB_HOST']}\n")
        cnf.write(f"port={port}\n")
        cnf.flush()
        path = Path(cnf.name)
    finally:
        cnf.close()
    os.chmod(path, 0o600)
    return path


def escape_mysql_ident(name: str) -> str:
    return name.replace("`", "``")


def mysql_run(
    cnf_path: Path,
    *,
    sql_stdin_path: Path | None = None,
    extra_args: list[str] | None = None,
) -> None:
    cmd = ["mysql", f"--defaults-extra-file={cnf_path}"]
    if extra_args:
        cmd.extend(extra_args)
    if sql_stdin_path is not None:
        with open(sql_stdin_path, "rb") as dump_in:
            proc = subprocess.run(
                cmd,
                stdin=dump_in,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
    else:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        out = proc.stdout.decode("utf-8", errors="replace").strip()
        parts = [p for p in (err, out) if p]
        raise RuntimeError(
            f"mysql failed (exit {proc.returncode}): {' | '.join(parts)}"
        )


def drop_database_if_exists(env: dict[str, str], cnf_path: Path) -> None:
    db = escape_mysql_ident(env["DB_NAME"])
    stmt = f"DROP DATABASE IF EXISTS `{db}`;"
    logging.info("Dropping existing database (if any): %s", env["DB_NAME"])
    proc = subprocess.run(
        [
            "mysql",
            f"--defaults-extra-file={cnf_path}",
            "-e",
            stmt,
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"DROP DATABASE failed (exit {proc.returncode}): {err}")


def restore_sql_file(env: dict[str, str], cnf_path: Path, sql_path: Path) -> None:
    extra = getenv_strip("MYSQL_RESTORE_EXTRA_ARGS", "")
    extra_list = extra.split() if extra else []
    logging.info("Applying SQL dump from %s", sql_path.name)
    mysql_run(cnf_path, sql_stdin_path=sql_path, extra_args=extra_list or None)


def download_backup_zip(
    env: dict[str, str],
    key: str,
    dest_zip: Path,
) -> None:
    client = s3_client(env)
    logging.info("Downloading s3://%s/%s", env["S3_BUCKET_NAME"], key)
    client.download_file(env["S3_BUCKET_NAME"], key, str(dest_zip))


def extract_single_sql(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        sql_names = [n for n in names if n.lower().endswith(".sql")]
        if not sql_names:
            raise RuntimeError("Archive contains no .sql file")
        if len(sql_names) > 1:
            raise RuntimeError(
                f"Archive contains multiple .sql files ({len(sql_names)}); "
                "expected exactly one."
            )
        member = sql_names[0]
        dest_sql = dest_dir / Path(member).name
        with zf.open(member) as src, open(dest_sql, "wb") as out:
            out.write(src.read())
    logging.info("Extracted SQL to %s (%s bytes)", dest_sql, dest_sql.stat().st_size)
    return dest_sql


def confirm_restore(db_name: str, backup_label: str) -> bool:
    print()
    print(
        f"This will DROP database `{db_name}` if it exists and restore from:\n"
        f"  {backup_label}\n"
        "All current data, tables, views, routines, triggers, and events in that "
        "database will be replaced."
    )
    raw = input("Type Y or yes to continue (anything else cancels): ").strip()
    if raw.lower() in ("y", "yes"):
        return True
    print("Restore cancelled.")
    return False


def prompt_choice(max_n: int) -> int | None:
    raw = input(f"Enter backup number [1-{max_n}] (empty = cancel): ").strip()
    if not raw:
        print("Cancelled.")
        return None
    try:
        n = int(raw)
    except ValueError:
        print("Invalid input: not a number.", file=sys.stderr)
        return None
    if n < 1 or n > max_n:
        print(f"Invalid choice: must be between 1 and {max_n}.", file=sys.stderr)
        return None
    return n


def main() -> int:
    load_env()
    env = require_restore_env()

    restore_log = getenv_strip("RESTORE_LOG_FILE", "restore.log") or "restore.log"
    log_path = Path(restore_log)
    if not log_path.is_absolute():
        log_path = script_dir() / log_path
    setup_logging(log_path)

    temp_dir: Path | None = None
    cnf_path: Path | None = None

    try:
        temp_dir = Path(tempfile.mkdtemp(prefix="mysql-restore-"))
        zip_local = temp_dir / "backup.sql.zip"
        logging.info("Listing backups from S3 (prefix=%r)", normalized_s3_prefix(env))
        try:
            rows = list_backups_newest_first(env)
        except (ClientError, BotoCoreError):
            logging.exception("Failed to list S3 objects")
            return 1

        if not rows:
            logging.error("No backups found for database %r under this prefix.", env["DB_NAME"])
            print(
                f"No backups found matching `{env['DB_NAME']}_*.sql.zip` "
                f"under prefix {normalized_s3_prefix(env)!r}.",
                file=sys.stderr,
            )
            return 1

        print()
        print("Available backups (newest first):")
        for i, row in enumerate(rows, start=1):
            lm = row["last_modified"]
            lm_s = lm.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(
                f"  {i:>3})  {Path(row['key']).name}  "
                f"  {lm_s}  ({format_size(row['size'])})"
            )

        choice = prompt_choice(len(rows))
        if choice is None:
            return 0

        picked = rows[choice - 1]
        label = picked["key"]
        if not confirm_restore(env["DB_NAME"], label):
            logging.info("User cancelled confirmation for %s", label)
            return 0

        cnf_path = write_mysql_defaults(env)
        download_backup_zip(env, picked["key"], zip_local)
        logging.info("Downloaded archive size: %s bytes", zip_local.stat().st_size)

        sql_path = extract_single_sql(zip_local, temp_dir)

        drop_database_if_exists(env, cnf_path)
        restore_sql_file(env, cnf_path, sql_path)

        logging.info("Restore completed successfully from %s", label)
        print("Restore finished successfully.")
        return 0
    except Exception:
        logging.exception("Restore failed")
        print("Restore failed; see log for details.", file=sys.stderr)
        return 1
    finally:
        if cnf_path is not None:
            try:
                cnf_path.unlink(missing_ok=True)
            except OSError as exc:
                logging.warning("Could not remove temporary mysql config: %s", exc)
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
