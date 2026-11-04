"""Experiment 3 – model operations: does monthly retraining help?

Replays the whole of year 2 (305 trading days) twice, once with THESIS_RETRAIN_POLICY=never and once with
=monthly (champion/challenger), and compares forecast error against the seasonal naive baseline.
Forecasts only use data from before the forecast day, so replaying all days in one run gives the same
forecasts as running day by day.

Usage (from the project folder):  .venv\\Scripts\\python experiments\\retraining.py
Results: experiments/results/retraining.md and .csv
"""

import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (ROOT, SOURCE_DAILY, bootstrap_quietly, markdown_table, run_child,  # noqa: E402
                    save_result, write_outputs)

POLICIES = ["never", "monthly"]


def child(policy: str) -> None:
    sys.path.insert(0, str(ROOT))
    from pipeline import config
    from pipeline.forecast import forecast_after
    from pipeline.ingest import process_landing
    from pipeline.transform import run_dbt
    from pipeline.warehouse import connect

    t0 = time.perf_counter()
    bootstrap_quietly()
    forecast_after(None)                         # initial model on year 1
    config.LANDING.mkdir(parents=True, exist_ok=True)
    for f in sorted(SOURCE_DAILY.glob("*.csv")):
        shutil.copyfile(f, config.LANDING / f.name)
    dates = [str(r.batch_date) for r in process_landing() if r.status == "loaded"]
    run_dbt(dates)
    forecast_after(dates)

    con = connect()
    try:
        overall = con.execute("""
            SELECT count(*), avg(model_abs_error), avg(baseline_abs_error)
            FROM ml.forecast_accuracy WHERE actual IS NOT NULL
        """).fetchone()
        monthly = con.execute("""
            SELECT strftime(target_date, '%Y-%m'), avg(model_abs_error), avg(baseline_abs_error)
            FROM ml.forecast_accuracy WHERE actual IS NOT NULL GROUP BY 1 ORDER BY 1
        """).fetchall()
        events = con.execute("""
            SELECT as_of_date::VARCHAR, event, champion_version, challenger_mae, champion_mae, message
            FROM ml.model_events ORDER BY event_time
        """).fetchall()
    finally:
        con.close()
    save_result({
        "policy": policy, "days": overall[0], "model_mae": overall[1], "baseline_mae": overall[2],
        "monthly": [{"month": m, "model_mae": a, "baseline_mae": b} for m, a, b in monthly],
        "events": [{"as_of": d, "event": e, "champion": v,
                    "challenger_mae": None if c is None else round(c), "champion_mae": None if p is None else round(p),
                    "message": msg} for d, e, v, c, p, msg in events],
        "seconds": round(time.perf_counter() - t0, 1),
    })


def main() -> None:
    runs = {p: run_child(Path(__file__), f"retrain_{p}", [p], {"THESIS_RETRAIN_POLICY": p}) for p in POLICIES}

    summary = []
    for p, r in runs.items():
        summary.append({
            "policy": p, "scored days": r["days"], "model MAE (£)": f"{r['model_mae']:,.0f}",
            "baseline MAE (£)": f"{r['baseline_mae']:,.0f}",
            "better than baseline": f"{1 - r['model_mae'] / r['baseline_mae']:.1%}",
            "model versions": 1 + sum(e["event"] == "promoted" for e in r["events"]),
            "replay time (s)": r["seconds"],
        })
    months = []
    for i, m in enumerate(runs["never"]["monthly"]):
        mo = runs["monthly"]["monthly"][i]
        months.append({
            "month": m["month"], "baseline": f"{m['baseline_mae']:,.0f}",
            "never retrain": f"{m['model_mae']:,.0f}", "monthly retrain": f"{mo['model_mae']:,.0f}",
            "best": min([("baseline", m["baseline_mae"]), ("never", m["model_mae"]),
                         ("monthly", mo["model_mae"])], key=lambda x: x[1])[0],
        })
    md = (
        "# Experiment 3 – retraining policy\n\n"
        "Year 2 replayed under each policy; MAE = mean absolute error of the next-trading-day revenue forecast.\n\n"
        + markdown_table(summary)
        + "\n## MAE by month (£)\n\n" + markdown_table(months)
        + "\n## Monthly policy: retraining decisions\n\n" + markdown_table(runs["monthly"]["events"])
    )
    write_outputs("retraining", summary, md)


if __name__ == "__main__":
    if "--child" in sys.argv:
        child(sys.argv[sys.argv.index("--child") + 1])
    else:
        main()
