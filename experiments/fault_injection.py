"""Experiment 1 – data quality and reliability (fault injection).

Sends deliberately broken files and rows through the real pipeline code (ingest + dbt) and checks
that every fault ends up where the design says it should:
  * file faults  -> rejected / skipped / loaded with a note (meta.ingestion_log)
  * row faults   -> quarantined with the right reason, dropped as duplicates, or flagged
  * batch faults -> loaded, but reported by silver.batch_anomalies
Then checks reliability: re-sent files are skipped, re-running a backfill and a full rebuild
give byte-identical silver and gold tables (table checksums), and measures the anomaly false-alarm rate
on clean history.

Usage (from the project folder):  .venv\\Scripts\\python experiments\\fault_injection.py
Results: experiments/results/fault_injection.md and .csv
"""

import random
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (ROOT, SOURCE_DAILY, bootstrap_quietly, markdown_table, read_day, run_child,  # noqa: E402
                    save_result, write_csv, write_outputs)

PRODUCT_CODE = re.compile(r"^\d{5}[A-Za-z]{0,2}$")
CHECKSUM_TABLES = [
    "silver.invoice_lines", "silver.quarantine_lines", "silver.batch_quality",
    "gold.fact_sales", "gold.daily_sales", "gold.daily_sales_by_country",
    "gold.dim_product", "gold.dim_customer", "gold.dim_date",
]
LOAD_TIME_COLUMNS = {"_silver_loaded_at", "_gold_loaded_at", "checked_at"}


def child() -> None:
    sys.path.insert(0, str(ROOT))
    from pipeline import config
    from pipeline.ingest import process_landing
    from pipeline.transform import run_dbt
    from pipeline.warehouse import connect

    def q(sql, params=()):
        con = connect()
        try:
            return con.execute(sql, list(params)).fetchall()
        finally:
            con.close()

    def run_pipeline():
        results = process_landing()
        dates = [str(r.batch_date) for r in results if r.status in ("loaded", "replaced")]
        if dates:
            run_dbt(dates)
        return {r.file_name: r for r in results}

    checks = []

    def check(fault, level, injected, expected, observed, ok):
        checks.append({"fault": fault, "level": level, "injected": injected, "expected": expected,
                       "observed": observed, "result": "PASS" if ok else "FAIL"})

    bootstrap_quietly()
    config.LANDING.mkdir(parents=True, exist_ok=True)
    days = sorted(p.stem for p in SOURCE_DAILY.glob("*.csv"))
    d1, d2, d3, d4, d5, d6, d7, d8, d9, d10 = days[:10]
    rng = random.Random(42)

    # ---------------------------------------------------------- round 1: clean day = baseline
    header, rows = read_day(d1)
    col = {c: i for i, c in enumerate(header)}
    write_csv(config.LANDING / f"{d1}.csv", header, rows)
    run_pipeline()
    base_reasons = dict(q("SELECT dq_reason, count(*) FROM silver.quarantine_lines WHERE batch_date = ? GROUP BY 1", [d1]))
    base_dups, base_late, base_unknown = q(
        "SELECT duplicates_dropped, late_arrival_lines, unknown_code_lines FROM silver.batch_quality WHERE batch_date = ?",
        [d1])[0]
    late_day = "2010-11-30"
    base_rev = dict(q("SELECT invoice_date::VARCHAR, net_product_revenue::DOUBLE FROM gold.daily_sales "
                      "WHERE invoice_date IN (?, ?)", [late_day, d1]))

    # ---------------------------------------------------------- round 2: row, file and batch faults
    counts = Counter(tuple(r) for r in rows)
    candidates = [r for r in rows if counts[tuple(r)] == 1 and r[col["Invoice"]].isdigit()
                  and PRODUCT_CODE.match(r[col["StockCode"]]) and float(r[col["Quantity"]]) > 0
                  and float(r[col["Price"]]) > 0 and r[col["Customer ID"]]]
    rng.shuffle(candidates)
    pool = iter(candidates)

    def mutated(**changes):
        r = list(next(pool))
        for key, value in changes.items():
            r[col[key]] = value
        return r

    row_faults = [
        ("invalid_price", 3, {"Price": "abc"}),
        ("invalid_quantity", 3, {"Quantity": "twelve"}),
        ("invalid_timestamp", 3, {"InvoiceDate": "2010-13-45 99:99:00"}),
        ("missing_invoice", 2, {"Invoice": ""}),
        ("missing_stock_code", 2, {"StockCode": ""}),
        ("future_date", 2, {"InvoiceDate": "2010-12-20 10:00:00"}),
        ("negative_price", 2, {"Price": "-5.00"}),
        ("test_product", 2, {"StockCode": "TEST001"}),
    ]
    extra = []
    for _, n, changes in row_faults:
        extra += [mutated(**changes) for _ in range(n)]
    for _ in range(2):                                  # cancellation invoice with a positive quantity
        r = list(next(pool))
        r[col["Invoice"]] = "C" + r[col["Invoice"]]
        extra.append(r)
    row_faults.append(("cancellation_with_positive_quantity", 2, None))
    duplicate_copies = [list(next(pool)) for _ in range(5)]
    late_rows = []
    for i in range(2):
        r = list(next(pool))
        r[col["Invoice"]], r[col["InvoiceDate"]] = f"99900{i}", f"{late_day} 14:00:00"
        late_rows.append(r)
    late_amount = sum(float(r[col["Quantity"]]) * float(r[col["Price"]]) for r in late_rows)
    unknown_code = [mutated(StockCode="ZZZ999")]
    write_csv(config.LANDING / f"{d1}.csv", header, rows + extra + duplicate_copies + late_rows + unknown_code)
    faulty_d1_bytes = (config.LANDING / f"{d1}.csv").read_bytes()

    h, r2 = read_day(d2)                                                        # renamed column
    write_csv(config.LANDING / f"{d2}.csv", ["UnitPrice" if c == "Price" else c for c in h], r2)
    h, r3 = read_day(d3)                                                        # not UTF-8
    r3[0][col["Description"]] += " £5"
    write_csv(config.LANDING / f"{d3}.csv", h, r3, encoding="cp1252")
    h, r4 = read_day(d4)                                                        # truncated mid-row
    write_csv(config.LANDING / f"{d4}.csv", h, r4)
    data = (config.LANDING / f"{d4}.csv").read_bytes()
    cut = len(data) // 2
    while data[cut - 1:cut] in (b"\n", b"\r"):
        cut += 7
    (config.LANDING / f"{d4}.csv").write_bytes(data[:cut])
    h, r5 = read_day(d5)                                                        # header only
    write_csv(config.LANDING / f"{d5}.csv", h, [])
    write_csv(config.LANDING / "orders_final.csv", h, r5)                       # bad file name
    h, r6 = read_day(d6)                                                        # extra column
    write_csv(config.LANDING / f"{d6}.csv", h + ["Discount"], [r + ["0"] for r in r6])
    extra_col_bytes = (config.LANDING / f"{d6}.csv").read_bytes()
    h, r7 = read_day(d7)                                                        # customer IDs lost
    write_csv(config.LANDING / f"{d7}.csv", h, [r[:col["Customer ID"]] + [""] + r[col["Customer ID"] + 1:] for r in r7])
    h, r8 = read_day(d8)                                                        # 95% of rows missing
    keep = sorted(rng.sample(range(len(r8)), max(1, len(r8) // 20)))
    write_csv(config.LANDING / f"{d8}.csv", h, [r8[i] for i in keep])
    h, r9 = read_day(d9)                                                        # volume x6
    spike = list(r9)
    for k in range(1, 6):
        for r in r9:
            c = list(r)
            c[col["Invoice"]] = f"{c[col['Invoice']]}{k}"
            spike.append(c)
    write_csv(config.LANDING / f"{d9}.csv", h, spike)
    h, r10 = read_day(d10)                                                      # 25% of rows sent twice
    clean_dups_d10 = len(r10) - len(set(map(tuple, r10)))
    added = [r10[i] for i in rng.sample(range(len(r10)), len(r10) // 4)]
    write_csv(config.LANDING / f"{d10}.csv", h, r10 + added)

    t0 = time.perf_counter()
    status = run_pipeline()
    round2_seconds = time.perf_counter() - t0

    def file_check(fault, name, expected_status, note=""):
        r = status.get(name)
        observed = f"{r.status}: {r.message}" if r else "not processed"
        check(fault, "file", 1, expected_status + note, observed[:120], bool(r) and r.status == expected_status)

    file_check("renamed column (Price -> UnitPrice)", f"{d2}.csv", "rejected")
    file_check("file not UTF-8 encoded", f"{d3}.csv", "rejected")
    file_check("file truncated mid-row", f"{d4}.csv", "rejected")
    file_check("header only, no data rows", f"{d5}.csv", "rejected")
    file_check("file name is not a date", "orders_final.csv", "rejected")
    file_check("unexpected extra column", f"{d6}.csv", "loaded", " (column noted)")
    file_check("corrected re-delivery of a loaded day", f"{d1}.csv", "replaced")

    reasons = dict(q("SELECT dq_reason, count(*) FROM silver.quarantine_lines WHERE batch_date = ? GROUP BY 1", [d1]))
    for reason, n, _ in row_faults:
        got = reasons.get(reason, 0) - base_reasons.get(reason, 0)
        check(f"row: {reason}", "row", n, f"{n} quarantined as {reason}", f"{got} quarantined", got == n)
    dups, late, unknown = q("SELECT duplicates_dropped, late_arrival_lines, unknown_code_lines "
                            "FROM silver.batch_quality WHERE batch_date = ?", [d1])[0]
    check("row: exact duplicate lines", "row", 5, "5 more duplicates dropped", f"{dups - base_dups} dropped",
          dups - base_dups == 5)
    check("row: unknown stock code", "row", 1, "loaded as 'other' + warning", f"{unknown - base_unknown} flagged",
          unknown - base_unknown == 1)
    check("row: late-arriving sale (dated day before)", "row", 2, "loaded, flagged late",
          f"{late - base_late} flagged late", late - base_late == 2)
    rev = dict(q("SELECT invoice_date::VARCHAR, net_product_revenue::DOUBLE FROM gold.daily_sales "
                 "WHERE invoice_date IN (?, ?)", [late_day, d1]))
    delta_late, delta_d1 = rev[late_day] - base_rev[late_day], rev[d1] - base_rev[d1]
    check("gold: late sale corrects the earlier day", "row", 2, f"{late_day} revenue +£{late_amount:,.2f}",
          f"+£{delta_late:,.2f}", abs(delta_late - late_amount) < 0.01)
    check("gold: faulty rows do not change revenue", "row", 21, f"{d1} revenue unchanged",
          f"change £{delta_d1:,.2f}", abs(delta_d1) < 0.01)

    anomalies = set(q("SELECT batch_date::VARCHAR, anomaly FROM silver.batch_anomalies WHERE batch_date >= ?", [d1]))
    for fault, day, anomaly in [("customer IDs missing in whole file", d7, "missing_customer_spike"),
                                ("95% of the day's rows missing", d8, "volume_drop"),
                                ("day's volume x6", d9, "volume_spike"),
                                ("25% of rows sent twice", d10, "duplicate_spike")]:
        check(f"batch: {fault}", "batch", 1, f"loaded + warning {anomaly}",
              "warned" if (day, anomaly) in anomalies else "not warned", (day, anomaly) in anomalies)
    d10_dups = q("SELECT duplicates_dropped FROM silver.batch_quality WHERE batch_date = ?", [d10])[0][0]
    check("batch: duplicates in the 25% re-sent rows dropped", "batch", len(added),
          f"{clean_dups_d10 + len(added)} dropped", f"{d10_dups} dropped", d10_dups == clean_dups_d10 + len(added))

    # ---------------------------------------------------------- round 3: exact re-sends
    rows_before = q("SELECT count(*) FROM silver.invoice_lines WHERE batch_date IN (?, ?)", [d1, d6])[0][0]
    (config.LANDING / f"{d1}.csv").write_bytes(faulty_d1_bytes)
    (config.LANDING / f"{d6}.csv").write_bytes(extra_col_bytes)
    status = run_pipeline()
    rows_after = q("SELECT count(*) FROM silver.invoice_lines WHERE batch_date IN (?, ?)", [d1, d6])[0][0]
    for name in (f"{d1}.csv", f"{d6}.csv"):
        r = status.get(name)
        check(f"exact re-send of {name}", "file", 1, "skipped_duplicate",
              r.status if r else "not processed", bool(r) and r.status == "skipped_duplicate")
    check("re-sends leave silver unchanged", "reliability", 2, f"{rows_before:,} rows", f"{rows_after:,} rows",
          rows_before == rows_after)

    # ---------------------------------------------------------- round 4: backfill and full rebuild
    def checksums():
        out = {}
        for table in CHECKSUM_TABLES:
            schema, name = table.split(".")
            cols = [c for (c,) in q("SELECT column_name FROM information_schema.columns WHERE table_schema = ? "
                                    "AND table_name = ? ORDER BY ordinal_position", [schema, name])
                    if c not in LOAD_TIME_COLUMNS]
            expr = ", ".join(f'"{c}"' for c in cols)
            n, s = q(f"SELECT count(*), coalesce(sum(hash({expr})::HUGEINT), 0)::VARCHAR FROM {table}")[0]
            out[table] = f"{n}:{s}"
        return out

    before = checksums()
    replay_dates = [d for (d,) in q("SELECT DISTINCT batch_date::VARCHAR FROM silver.batch_quality WHERE batch_date >= ?", [d1])]
    t0 = time.perf_counter()
    run_dbt(replay_dates)
    backfill_seconds = time.perf_counter() - t0
    after_backfill = checksums()
    t0 = time.perf_counter()
    run_dbt(None, full_refresh=True)
    rebuild_seconds = time.perf_counter() - t0
    after_rebuild = checksums()
    same_backfill = [t for t in CHECKSUM_TABLES if before[t] == after_backfill[t]]
    same_rebuild = [t for t in CHECKSUM_TABLES if before[t] == after_rebuild[t]]
    check("backfill of all replayed days", "reliability", len(replay_dates),
          f"{len(CHECKSUM_TABLES)} tables identical", f"{len(same_backfill)} identical",
          len(same_backfill) == len(CHECKSUM_TABLES))
    check("full rebuild from bronze", "reliability", 1, f"{len(CHECKSUM_TABLES)} tables identical",
          f"{len(same_rebuild)} identical", len(same_rebuild) == len(CHECKSUM_TABLES))

    history = q("SELECT count(*) FROM silver.batch_quality WHERE batch_date < ?", [d1])[0][0]
    false_alarms = q("SELECT batch_date::VARCHAR, anomaly FROM silver.batch_anomalies WHERE batch_date < ? "
                     "ORDER BY 1", [d1])
    save_result({
        "checks": checks,
        "false_alarms": [{"batch_date": d, "anomaly": a} for d, a in false_alarms],
        "history_batches": history,
        "timings": {"round2_ingest_and_build_s": round(round2_seconds, 1),
                    "backfill_s": round(backfill_seconds, 1), "full_rebuild_s": round(rebuild_seconds, 1)},
    })


def main() -> None:
    data = run_child(Path(__file__), "faults")
    checks = data["checks"]
    passed = sum(c["result"] == "PASS" for c in checks)
    by_level = Counter(c["level"] for c in checks)
    fa = data["false_alarms"]
    md = (
        "# Experiment 1 – fault injection\n\n"
        f"**{passed} of {len(checks)} checks passed** "
        f"({', '.join(f'{n} {lvl}' for lvl, n in by_level.items())}).\n\n"
        + markdown_table(checks)
        + f"\n## Batch anomaly false alarms on clean history\n\n"
        f"{len(fa)} warnings on {data['history_batches']} clean history batches "
        f"({len(fa) / data['history_batches']:.1%}):\n\n" + markdown_table(fa)
        + "\n## Timings\n\n" + markdown_table([data["timings"]])
    )
    write_outputs("fault_injection", checks, md)


if __name__ == "__main__":
    child() if "--child" in sys.argv else main()
