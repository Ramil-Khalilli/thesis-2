"""Prefect flows and the command line entry point.

Usage (from the project folder, with the .venv Python):
    python -m pipeline.flows bootstrap               load year 1 history, build silver + gold, train first model (once)
    python -m pipeline.flows run                     daily cycle: landing -> bronze -> silver -> gold -> forecast
    python -m pipeline.flows evaluate                forecast accuracy vs baseline, retraining decisions
    python -m pipeline.flows ingest                  landing -> bronze only
    python -m pipeline.flows transform               rebuild silver + gold for all dates
    python -m pipeline.flows backfill START END      rebuild silver + gold for a date range (YYYY-MM-DD)
    python -m pipeline.flows status                  show what has been loaded
    python -m pipeline.flows reset                   delete all pipeline output and restart the simulator
"""

import shutil
import subprocess
import sys
import time
from datetime import datetime

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from . import config
from .ingest import bootstrap_history, process_landing
from .transform import run_dbt
from .warehouse import connect

LOADED = ("loaded", "replaced")


@task(cache_policy=NO_CACHE)
def ingest_landing_task(run_id: str):
    return process_landing(run_id)


@task(cache_policy=NO_CACHE)
def transform_task(batch_dates: list[str] | None = None, full_refresh: bool = False):
    run_dbt(batch_dates, full_refresh)


@task(cache_policy=NO_CACHE)
def forecast_task(batch_dates: list[str] | None = None):
    from .forecast import forecast_after
    logger = get_run_logger()
    lines = forecast_after(batch_dates)
    for line in lines[-15:]:
        logger.info(line)
    if len(lines) > 15:
        logger.info(f"... {len(lines) - 15} earlier lines not shown")


def _log_results(results) -> None:
    logger = get_run_logger()
    if not results:
        logger.info("Landing is empty; nothing to do.")
    for r in results:
        line = f"{r.file_name}: {r.status}"
        if r.rows_loaded is not None:
            line += f" ({r.rows_loaded:,} rows)"
        if r.message:
            line += f" - {r.message}"
        (logger.warning if r.status == "rejected" else logger.info)(line)


def _run_id() -> str:
    from prefect.runtime import flow_run
    return str(flow_run.id)[:12]


def _record_run(run_id, flow_name, started, timings, results, status, message="") -> None:
    con = connect()
    try:
        con.execute(
            "INSERT INTO meta.pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, flow_name, started, datetime.now(), len(results),
             sum(r.status in LOADED for r in results),
             sum(r.rows_loaded or 0 for r in results if r.status in LOADED),
             timings.get("ingest"), timings.get("transform"), timings.get("forecast"), status, message],
        )
    finally:
        con.close()


@flow(name="daily-run")
def daily_flow():
    """The daily cycle: load new files into bronze, then rebuild silver, gold and the forecast for those days."""
    run_id, started, timings, results = _run_id(), datetime.now(), {}, []

    def timed(step, fn, *args):
        t0 = time.perf_counter()
        out = fn(*args)
        timings[step] = round(time.perf_counter() - t0, 2)
        return out

    try:
        results = timed("ingest", ingest_landing_task, run_id)
        _log_results(results)
        dates = [str(r.batch_date) for r in results if r.status in LOADED]
        if dates:
            get_run_logger().info(f"Building silver and gold for {len(dates)} batch date(s): {', '.join(sorted(dates))}")
            timed("transform", transform_task, dates)
            timed("forecast", forecast_task, dates)
    except Exception as e:
        _record_run(run_id, "daily-run", started, timings, results, "failed", str(e)[:500])
        raise
    _record_run(run_id, "daily-run", started, timings, results, "success")
    return results


@flow(name="ingest-landing")
def ingest_flow():
    results = ingest_landing_task(_run_id())
    _log_results(results)
    return results


@flow(name="transform")
def transform_flow(batch_dates: list[str] | None = None, full_refresh: bool = False):
    transform_task(batch_dates, full_refresh)


@flow(name="bootstrap-history")
def bootstrap_flow(force: bool = False):
    get_run_logger().info(bootstrap_history(force))
    transform_task(None, full_refresh=True)
    forecast_task(None)   # trains the first model on year 1 and forecasts the first replay day


def bronze_dates_between(start: str, end: str) -> list[str]:
    con = connect()
    try:
        rows = con.execute(
            "SELECT DISTINCT batch_date FROM bronze.invoice_lines WHERE batch_date BETWEEN ? AND ? ORDER BY 1",
            [start, end],
        ).fetchall()
    finally:
        con.close()
    return [str(r[0]) for r in rows]


def status() -> None:
    con = connect()
    try:
        print(con.execute("""
            SELECT status, count(*) AS files, coalesce(sum(rows_loaded), 0) AS rows_loaded
            FROM meta.ingestion_log GROUP BY status ORDER BY status
        """).df().to_string(index=False))
        print()
        print(con.execute("""
            SELECT batch_date, file_name, status, rows_loaded, message
            FROM meta.ingestion_log ORDER BY finished_at DESC LIMIT 10
        """).df().to_string(index=False))
        tables = {r[0] for r in con.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables").fetchall()}
        if "bronze.invoice_lines" in tables:
            days, rows, last = con.execute(
                "SELECT count(DISTINCT batch_date), count(*), max(batch_date) FROM bronze.invoice_lines"
            ).fetchone()
            print(f"\nbronze.invoice_lines: {rows:,} rows, {days} days, latest {last}")
        if "silver.batch_quality" in tables:
            print("\nsilver, last 5 batches:")
            print(con.execute("""
                SELECT batch_date, rows_bronze, duplicates_dropped AS dups, rows_quarantined AS quarantined,
                       rows_silver, cancellation_lines AS cancels, missing_customer_lines AS no_customer
                FROM silver.batch_quality ORDER BY batch_date DESC LIMIT 5
            """).df().to_string(index=False))
        if "gold.daily_sales" in tables:
            print("\ngold.daily_sales, last 5 days:")
            print(con.execute("""
                SELECT invoice_date, round(net_product_revenue, 2) AS net_revenue, invoices, customers,
                       new_customers, avg_invoice_value, cancellation_rate
                FROM gold.daily_sales ORDER BY invoice_date DESC LIMIT 5
            """).df().to_string(index=False))
    finally:
        con.close()


def reset() -> None:
    for path in (config.DATA / "bronze", config.ARCHIVE, config.QUARANTINE):
        shutil.rmtree(path, ignore_errors=True)
    config.WAREHOUSE.unlink(missing_ok=True)
    shutil.rmtree(config.DATA / "mlartifacts", ignore_errors=True)
    (config.DATA / "mlflow.db").unlink(missing_ok=True)
    subprocess.run([sys.executable, str(config.ROOT / "simulator" / "deliver.py"), "--reset"], check=True)
    print("Pipeline output deleted. Run 'bootstrap' to start again.")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    if command == "bootstrap":
        bootstrap_flow(force="--force" in sys.argv)
    elif command == "run":
        daily_flow()
    elif command == "ingest":
        ingest_flow()
    elif command == "transform":
        transform_flow(None, full_refresh="--full-refresh" in sys.argv)
    elif command == "backfill" and len(sys.argv) == 4:
        dates = bronze_dates_between(sys.argv[2], sys.argv[3])
        print(f"Backfilling {len(dates)} batch date(s).")
        if dates:
            transform_flow(dates)
    elif command == "status":
        status()
    elif command == "evaluate":
        from .forecast import evaluation_summary
        print(evaluation_summary())
    elif command == "reset":
        reset()
    else:
        sys.exit(__doc__)
