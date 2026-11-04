"""Paths and constants shared by the pipeline."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

SOURCE_INITIAL = DATA / "source" / "initial"   # year 1 history (bootstrap)
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
