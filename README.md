# Thesis 2 – Automated batch data pipeline (Online Retail II)

## Folder layout
```
online_retail_II.csv          raw dataset (UCI Online Retail II, Kaggle CSV export)
Data_Exploration.ipynb        data audit and cleaning rules
requirements.txt              Python packages (install into .venv)
simulator/
  split_days.py               prepares the source data (run once)
  deliver.py                  delivers one trading day per run into data/landing/
pipeline/
  config.py                   paths and required columns
  warehouse.py                DuckDB connection, ingestion log, bronze view
  ingest.py                   landing -> bronze rules
  transform.py                runs dbt from Python
  flows.py                    Prefect flows + command line
transform/                    dbt project (bronze -> silver -> gold)
  models/silver/              cleaning rules as SQL models
  models/gold/                dimensions, sales fact, daily KPIs
  seeds/                      UK bank holidays 2009–2011
  tests/                      reconciliation and warning tests
  macros/                     batch selection + replace-by-batch logic
data/                         (all generated; can be rebuilt from the raw CSV)
  source/initial/             year 1 (Dec 2009 – Nov 2010): initial load + model training
  source/daily/               year 2 (Dec 2010 – Dec 2011): one CSV per trading day, 305 files
  source/manifest.csv         date, file name and row count of each daily file
  landing/                    where new files arrive
  bronze/invoice_lines/       raw Parquet, one folder per day (batch_date=YYYY-MM-DD)
  archive/                    processed landing files (and skipped duplicates)
  quarantine/files/           files the pipeline refused to load
  warehouse.duckdb            ingestion log + views
  simulator_state.json        last day delivered
```

## Setup (once)
```
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python simulator\split_days.py
.venv\Scripts\python -m pipeline.flows bootstrap
```

## Daily cycle
```
.venv\Scripts\python simulator\deliver.py            # a new day arrives in landing
.venv\Scripts\python -m pipeline.flows run            # landing -> bronze -> silver -> gold
.venv\Scripts\python -m pipeline.flows status         # what has been loaded
```
Other commands:
| Command | What it does |
|---|---|
| `deliver.py --days 5` / `--status` / `--reset` | deliver several days / show progress / start over |
| `pipeline.flows ingest` | landing -> bronze only |
| `pipeline.flows transform [--full-refresh]` | rebuild silver for all dates |
| `pipeline.flows backfill 2010-11-01 2010-11-30` | rebuild silver for a date range |
| `pipeline.flows reset` | delete all pipeline output and restart the simulator |

## What the simulator changes in the data
Only one thing: the raw CSV contains 1–9 Dec 2010 twice (two Excel sheets stacked
together, 22,523 identical rows each), so one copy is removed. Every other data issue
(11,812 duplicate lines, cancellations, missing customer IDs, non-product codes, ...)
is left in on purpose for the pipeline to handle.

## Landing -> bronze rules
| Situation | Result |
|---|---|
| New file `YYYY-MM-DD.csv` | Loaded as text into `bronze/invoice_lines/batch_date=.../`, file archived |
| Exact same file sent again (same SHA-256) | `skipped_duplicate`, nothing loaded twice |
| New version of an already loaded day | `replaced`: the day's folder is swapped, never doubled |
| Required column missing or renamed | `rejected`, file moved to quarantine |
| File name is not a date | `rejected`, file moved to quarantine |
| Rows written ≠ rows in file | `rejected` (nothing written) |

Every decision is a row in `meta.ingestion_log`. Each bronze row carries lineage columns:
`_source_file`, `_file_hash`, `_ingested_at`, `_row_number`.

## Bronze -> silver (dbt)
Silver is rebuilt only for the batch dates loaded in the current run. For each of those dates the old
silver rows are deleted and the new ones inserted, so reprocessing a day replaces it and never doubles it.

| Table | Contents |
|---|---|
| `silver.invoice_lines` | typed, valid, deduplicated lines with `line_type` and flags (`is_cancellation`, `is_missing_customer`, `is_free_item`, `is_extreme_quantity`, `is_late_arrival`) |
| `silver.quarantine_lines` | rows that failed validation, with `dq_reason` and the original text values |
| `silver.batch_quality` | per day: bronze rows, duplicates dropped, quarantined, silver rows and counts of each issue |

`line_type`: product, stock_correction, adjustment, postage, manual, discount, sample, fee, voucher,
other (unknown code, reported by a warning test).

Validation reasons: missing_invoice, missing_stock_code, invalid_quantity, invalid_price,
invalid_timestamp, future_date, test_product, negative_price, cancellation_with_positive_quantity.

Tests run on every build (`dbt build`): not-null and accepted-value checks, and a reconciliation
test that every bronze row is either in silver, in quarantine or counted as a dropped duplicate.

Full replay check (all 604 days): 1,044,848 bronze rows = 11,812 duplicates + 18 quarantined +
1,033,018 silver rows, matching the audit in Data_Exploration.ipynb.

## Silver -> gold (dbt)
| Table | Contents | Update |
|---|---|---|
| `gold.fact_sales` | product sale and cancellation lines with product version, customer, `has_matching_sale` for cancellations | replaced per batch date |
| `gold.daily_sales` | KPIs per invoice date: gross/net revenue, cancellations, invoices, customers, new customers, units, avg invoice value, non-UK share | invoice dates touched by the batch are recomputed |
| `gold.daily_sales_by_country` | net revenue, invoices, customers per date and country | same as daily_sales |
| `gold.dim_product` | SCD Type 2: one row per description version (`valid_from` inclusive, `valid_to` exclusive, `is_current`), plus typical pack size and median price | rebuilt each run |
| `gold.dim_customer` | first/last purchase, invoices, net revenue, latest country | rebuilt each run |
| `gold.dim_date` | calendar, trading days, UK bank holidays, days to Christmas | rebuilt each run |

Product versions: a new version starts when a product gets a description it never had before
(411 of 4,147 products were renamed in year 1). Switching back to an older name does not create a version.

Late data: because daily KPIs are recomputed for every invoice date in the batch, a row that arrives in a
later file updates the day it belongs to (tested: a sale dated 2 Dec delivered in the 5 Dec file raised
2 Dec's net revenue by exactly its amount).

Gold tests: unique and not-null keys, every sale linked to a product version and a calendar day, one
current version per product with no gaps or overlaps, and gold revenue totals equal to silver.

## Notes
* Each Prefect run currently starts a temporary local server (~20 s overhead). Running a
  permanent one (`prefect server start` in its own terminal) removes that and gives the
  Prefect UI at http://127.0.0.1:4200.
* "nanobind: leaked function" messages at the end of a Prefect run are harmless shutdown
  noise (DuckDB 2.0 + Prefect); the data is unaffected.
