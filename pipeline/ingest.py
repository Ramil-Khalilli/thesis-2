"""Landing -> bronze ingestion.

Rules for every file found in data/landing/:
  * The name must be YYYY-MM-DD.csv (the batch date). Otherwise -> rejected.
  * A file whose exact content (SHA-256 hash) was already loaded is skipped, so
    re-sent files are never loaded twice.
  * The header must contain all required columns. Otherwise -> rejected.
    Extra columns are accepted and noted in the log.
  * Values are stored as text, unchanged, plus lineage columns
    (_source_file, _file_hash, _ingested_at, _row_number). Typing and cleaning
    happen later in silver.
  * Each batch date has one bronze folder. A new version of an already loaded day
    replaces the old one (status "replaced"), so re-running a day never doubles it.
  * The number of rows written must equal the number of rows in the file.

Processed files move to data/archive/, refused files to data/quarantine/.
Every decision is recorded in meta.ingestion_log.
"""

import csv
import hashlib
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from . import config
from .warehouse import connect, refresh_bronze_view

FILE_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")


@dataclass
class IngestResult:
    file_name: str
    status: str
    batch_date: date | None = None
    rows_in_file: int | None = None
    rows_loaded: int | None = None
    message: str = ""


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_header_and_count(path: Path) -> tuple[list[str], int]:
    """Header and number of data rows (quoted line breaks handled correctly)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = sum(1 for row in reader if row)
    return header, rows


def landing_files() -> list[Path]:
    """Files waiting in landing, oldest batch first. Half-written *.part files are ignored."""
    config.LANDING.mkdir(parents=True, exist_ok=True)
    return sorted(p for p in config.LANDING.iterdir() if p.is_file() and not p.name.endswith(".part"))


def _log(con, run_id, result: IngestResult, digest, started) -> None:
    con.execute(
        "INSERT INTO meta.ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, result.file_name, digest, result.batch_date, result.status,
         result.rows_in_file, result.rows_loaded, result.message, started, datetime.now()],
    )


def _move(path: Path, folder: Path, new_name: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(folder / new_name))


def ingest_file(con: duckdb.DuckDBPyConnection, path: Path, run_id: str) -> IngestResult:
    started = datetime.now()
    digest = None

    def reject(message: str, batch: date | None = None) -> IngestResult:
        stamp = started.strftime("%Y%m%dT%H%M%S")
        _move(path, config.QUARANTINE / "files", f"{stamp}__{path.name}")
        result = IngestResult(path.name, "rejected", batch, message=message)
        _log(con, run_id, result, digest, started)
        return result

    match = FILE_NAME.match(path.name)
    if not match:
        return reject("file name is not YYYY-MM-DD.csv")
    try:
        batch_date = date.fromisoformat(match.group(1))
    except ValueError:
        return reject("file name is not a valid date")

    digest = file_hash(path)
    seen = con.execute(
        "SELECT batch_date FROM meta.ingestion_log "
        "WHERE file_hash = ? AND status IN ('loaded', 'replaced') LIMIT 1", [digest]
    ).fetchone()
    if seen:
        _move(path, config.ARCHIVE / "duplicates", f"{batch_date}__{digest[:8]}__{started:%Y%m%dT%H%M%S}.csv")
        result = IngestResult(path.name, "skipped_duplicate", batch_date,
                              message=f"identical file already loaded for {seen[0]}")
        _log(con, run_id, result, digest, started)
        return result

    try:
        header, rows_in_file = read_header_and_count(path)
    except (UnicodeDecodeError, csv.Error) as e:
        return reject(f"file cannot be read as UTF-8 CSV: {e}", batch_date)

    missing = [c for c in config.REQUIRED_COLUMNS if c not in header]
    if missing:
        return reject(f"missing required columns: {missing}", batch_date)
    extra = [c for c in header if c not in config.REQUIRED_COLUMNS]

    # Write to a temporary folder first, check it, then swap it in.
    partition = config.BRONZE / f"batch_date={batch_date}"
    tmp = config.BRONZE / f"_tmp_{batch_date}_{digest[:8]}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    out_file = tmp / f"part-{digest[:8]}.parquet"

    con.execute(f"""
        COPY (
            SELECT *,
                   {_sql_text(path.name)}   AS _source_file,
                   {_sql_text(digest)}      AS _file_hash,
                   TIMESTAMP {_sql_text(started.isoformat(sep=' '))} AS _ingested_at,
                   row_number() OVER ()     AS _row_number
            FROM read_csv({_sql_text(path.as_posix())}, all_varchar = true, header = true)
        ) TO {_sql_text(out_file.as_posix())} (FORMAT parquet)
    """)
    rows_loaded = con.execute(f"SELECT count(*) FROM read_parquet({_sql_text(out_file.as_posix())})").fetchone()[0]

    if rows_loaded != rows_in_file:
        shutil.rmtree(tmp, ignore_errors=True)
        return reject(f"row count mismatch: file has {rows_in_file}, parsed {rows_loaded}", batch_date)

    replaced = partition.exists()
    if replaced:
        shutil.rmtree(partition)
    tmp.rename(partition)

    _move(path, config.ARCHIVE / "landing", f"{batch_date}__{digest[:8]}.csv")

    message = "new version of this day replaced the previous one" if replaced else ""
    if extra:
        message = (message + "; " if message else "") + f"extra columns kept: {extra}"
    result = IngestResult(path.name, "replaced" if replaced else "loaded", batch_date,
                          rows_in_file, rows_loaded, message)
    _log(con, run_id, result, digest, started)
    return result


def process_landing(run_id: str | None = None) -> list[IngestResult]:
    """Ingest every file currently in landing."""
    run_id = run_id or uuid.uuid4().hex[:12]
    con = connect()
    try:
        results = [ingest_file(con, f, run_id) for f in landing_files()]
        refresh_bronze_view(con)
    finally:
        con.close()
    return results


def bootstrap_history(force: bool = False) -> str:
    """Load the year 1 history file into bronze, split into one folder per day."""
    files = sorted(config.SOURCE_INITIAL.glob("*.csv"))
    if len(files) != 1:
        raise SystemExit(f"Expected one history file in {config.SOURCE_INITIAL}, found {len(files)}. "
                         "Run: python simulator/split_days.py")
    path = files[0]
    if any(config.BRONZE.glob("batch_date=*")) and not force:
        return "Bronze already has data; bootstrap skipped (use --force to reload)."

    run_id = "bootstrap-" + uuid.uuid4().hex[:6]
    started = datetime.now()
    digest = file_hash(path)
    _, rows_in_file = read_header_and_count(path)

    con = connect()
    try:
        tmp = config.BRONZE.parent / "_tmp_bootstrap"
        shutil.rmtree(tmp, ignore_errors=True)
        config.BRONZE.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
                SELECT *,
                       {_sql_text(path.name)} AS _source_file,
                       {_sql_text(digest)}    AS _file_hash,
                       TIMESTAMP {_sql_text(started.isoformat(sep=' '))} AS _ingested_at,
                       row_number() OVER ()   AS _row_number,
                       CAST(CAST(InvoiceDate AS TIMESTAMP) AS DATE) AS batch_date
                FROM read_csv({_sql_text(path.as_posix())}, all_varchar = true, header = true)
            ) TO {_sql_text(tmp.as_posix())} (FORMAT parquet, PARTITION_BY (batch_date))
        """)
        rows_loaded = con.execute(
            f"SELECT count(*) FROM read_parquet({_sql_text((tmp / '*' / '*.parquet').as_posix())})"
        ).fetchone()[0]
        if rows_loaded != rows_in_file:
            shutil.rmtree(tmp, ignore_errors=True)
            raise SystemExit(f"Bootstrap row count mismatch: file {rows_in_file}, parsed {rows_loaded}")

        config.BRONZE.mkdir(parents=True, exist_ok=True)
        days = 0
        for part in sorted(tmp.iterdir()):
            target = config.BRONZE / part.name
            shutil.rmtree(target, ignore_errors=True)
            part.rename(target)
            days += 1
        shutil.rmtree(tmp, ignore_errors=True)

        _log(con, run_id, IngestResult(path.name, "loaded", None, rows_in_file, rows_loaded,
                                       f"bootstrap: {days} days"), digest, started)
        refresh_bronze_view(con)
    finally:
        con.close()
    return f"Bootstrap loaded {rows_loaded:,} rows into {days} daily bronze folders."


if __name__ == "__main__":
    for r in process_landing():
        print(r)
