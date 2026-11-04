"""DuckDB connection, metadata tables and views."""

import duckdb

from . import config


def connect() -> duckdb.DuckDBPyConnection:
    config.DATA.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.WAREHOUSE))
    con.execute("CREATE SCHEMA IF NOT EXISTS meta")
    con.execute("""
        CREATE TABLE IF NOT EXISTS meta.ingestion_log (
            run_id        VARCHAR,
            file_name     VARCHAR,
            file_hash     VARCHAR,
            batch_date    DATE,
            status        VARCHAR,   -- loaded | replaced | skipped_duplicate | rejected
            rows_in_file  BIGINT,
            rows_loaded   BIGINT,
            message       VARCHAR,
            started_at    TIMESTAMP,
            finished_at   TIMESTAMP
        )
    """)
    return con


def refresh_bronze_view(con: duckdb.DuckDBPyConnection) -> None:
    """Expose all bronze Parquet files as one table: bronze.invoice_lines."""
    if not any(config.BRONZE.glob("batch_date=*/*.parquet")):
        return
    pattern = (config.BRONZE / "batch_date=*" / "*.parquet").as_posix()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute(f"""
        CREATE OR REPLACE VIEW bronze.invoice_lines AS
        SELECT * FROM read_parquet('{pattern}', hive_partitioning = true, union_by_name = true)
    """)
