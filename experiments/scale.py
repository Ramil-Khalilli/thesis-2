"""Experiment 2 – scalability.

Runs the same three daily batches at x1, x10 and x100 their real volume and times each step.
Enlarged files are made by copying every row k times with a new invoice number and customer ID
(so the copies are not duplicates and are not removed by deduplication). The year 1 history stays
at its real size, so the test measures the cost of a bigger daily batch on top of the same warehouse.

Usage (from the project folder):  .venv\\Scripts\\python experiments\\scale.py
Results: experiments/results/scale.md and .csv
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (ROOT, SOURCE_DAILY, bootstrap_quietly, markdown_table, read_day, run_child,  # noqa: E402
                    save_result, write_csv, write_outputs)

FACTORS = [1, 10, 100]
DAYS = 3


def enlarge(header, rows, factor):
    inv, cust = header.index("Invoice"), header.index("Customer ID")
    out = list(rows)
    for k in range(1, factor):
        for r in rows:
            c = list(r)
            c[inv] = f"{c[inv]}{k:03d}"
            if c[cust]:
                c[cust] = f"{int(float(c[cust]))}{k:03d}.0"
            out.append(c)
    return out


def child(factor: int) -> None:
    sys.path.insert(0, str(ROOT))
    from pipeline import config
    from pipeline.ingest import process_landing
    from pipeline.transform import TRANSFORM, run_dbt

    bootstrap_quietly()
    config.LANDING.mkdir(parents=True, exist_ok=True)
    measurements = []
    for day in sorted(p.stem for p in SOURCE_DAILY.glob("*.csv"))[:DAYS]:
        header, rows = read_day(day)
        big = enlarge(header, rows, factor)
        path = config.LANDING / f"{day}.csv"
        write_csv(path, header, big)
        size_mb = path.stat().st_size / 1e6

        t0 = time.perf_counter()
        results = process_landing()
        ingest_s = time.perf_counter() - t0
        assert results[0].status == "loaded", results[0]

        t0 = time.perf_counter()
        run_dbt([day])
        transform_s = time.perf_counter() - t0

        nodes = json.loads((TRANSFORM / "target" / "run_results.json").read_text())["results"]
        models = sorted((n for n in nodes if n["unique_id"].startswith("model.")),
                        key=lambda n: -n["execution_time"])
        measurements.append({
            "factor": factor, "day": day, "rows": len(big), "file_mb": round(size_mb, 1),
            "ingest_s": round(ingest_s, 2), "transform_s": round(transform_s, 2),
            "slowest_models": ", ".join(f"{n['unique_id'].split('.')[-1]} {n['execution_time']:.1f}s"
                                        for n in models[:3]),
        })
    save_result({"measurements": measurements})


def main() -> None:
    measurements = []
    for factor in FACTORS:
        measurements += run_child(Path(__file__), f"scale_x{factor}", [str(factor)])["measurements"]

    summary = []
    for factor in FACTORS:
        m = [x for x in measurements if x["factor"] == factor]
        rows = sum(x["rows"] for x in m) / len(m)
        ingest = sum(x["ingest_s"] for x in m) / len(m)
        transform = sum(x["transform_s"] for x in m) / len(m)
        summary.append({
            "volume": f"x{factor}", "rows per day": f"{rows:,.0f}",
            "file MB": round(sum(x["file_mb"] for x in m) / len(m), 1),
            "ingest s": round(ingest, 2), "transform s": round(transform, 2),
            "total s": round(ingest + transform, 2),
            "rows per second": f"{rows / (ingest + transform):,.0f}",
        })
    base = summary[0]["total s"]
    for s in summary:
        s["time vs x1"] = f"{s['total s'] / base:.1f}x"

    md = (
        "# Experiment 2 – scalability\n\n"
        f"Average of {DAYS} daily batches per volume. Ingest = landing -> bronze; transform = dbt build of "
        "silver + gold for that day, including all tests. Year 1 history stays at its real size.\n\n"
        + markdown_table(summary)
        + "\n## Per batch (with the three slowest dbt models)\n\n" + markdown_table(measurements)
    )
    write_outputs("scale", measurements, md)


if __name__ == "__main__":
    if "--child" in sys.argv:
        child(int(sys.argv[sys.argv.index("--child") + 1]))
    else:
        main()
