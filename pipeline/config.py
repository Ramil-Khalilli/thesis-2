"""Paths and constants shared by the pipeline."""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Both can be overridden, so experiments run in their own folder without touching the main warehouse.
DATA = Path(os.environ.get("THESIS_DATA_DIR", ROOT / "data"))
SOURCE = Path(os.environ.get("THESIS_SOURCE_DIR", ROOT / "data" / "source"))

SOURCE_INITIAL = SOURCE / "initial"            # year 1 history (bootstrap)
LANDING = DATA / "landing"                     # new daily files arrive here
BRONZE = DATA / "bronze" / "invoice_lines"     # raw Parquet, one folder per batch date
ARCHIVE = DATA / "archive"                     # landing files after processing
QUARANTINE = DATA / "quarantine"               # files the pipeline refused to load
WAREHOUSE = DATA / "warehouse.duckdb"          # DuckDB database (logs, views, later silver/gold)

# Columns every incoming file must have (names as in the source CSV).
REQUIRED_COLUMNS = [
    "Invoice", "StockCode", "Description", "Quantity",
    "InvoiceDate", "Price", "Customer ID", "Country",
]
