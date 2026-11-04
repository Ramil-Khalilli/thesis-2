"""Prepare the source data for the pipeline simulation.

Reads the raw Online Retail II CSV and produces:
  data/source/initial/history_until_<date>.csv   year 1, used for the initial load and model training
  data/source/daily/YYYY-MM-DD.csv               one file per trading day, replayed by deliver.py
  data/source/manifest.csv                       date, file and row count of every daily file

The raw CSV is two Excel sheets stacked together, and both sheets contain 1-9 Dec 2010.
This script removes that export overlap (one copy only). All other data issues
(duplicate lines, cancellations, missing customer IDs, ...) are left in on purpose:
handling them is the pipeline's job.

Values are kept as the original text, so the daily files look exactly like the source.

Usage (from the project folder):
    python simulator/split_days.py
"""

import argparse
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def remove_sheet_overlap(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the first sheet's copy of the days that the second sheet repeats."""
    dates = pd.to_datetime(df["InvoiceDate"])
    jumps_back = dates.diff().dt.total_seconds() < -86400
    if not jumps_back.any():
        print("No sheet boundary found; nothing to remove.")
        return df

    boundary = df.index[jumps_back][0]
    sheet2_start = dates.loc[boundary].normalize()
    in_sheet1 = df.index < boundary
    overlap1 = df[in_sheet1 & (dates >= sheet2_start)]
    overlap2 = df[~in_sheet1 & (dates < dates.loc[boundary - 1].normalize() + pd.Timedelta(days=1))]

    cols = list(df.columns)
    same = (overlap1.sort_values(cols).reset_index(drop=True)
            .equals(overlap2.sort_values(cols).reset_index(drop=True)))
    if not same:
        raise SystemExit("The overlapping days differ between the two sheets; stopping so nothing is lost.")

    print(f"Sheet overlap: {sheet2_start.date()} to {dates.loc[boundary - 1].date()}, "
          f"{len(overlap1):,} rows appear twice -> removing one copy.")
    return df.drop(index=overlap1.index).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=str(ROOT / "online_retail_II.csv"), help="raw CSV file")
    parser.add_argument("--out", default=str(ROOT / "data" / "source"), help="output folder")
    parser.add_argument("--replay-start", default="2010-12-01", help="first day delivered by the simulator")
    args = parser.parse_args()

    out = Path(args.out)
    replay_start = pd.Timestamp(args.replay_start)

    print(f"Reading {args.source} ...")
    df = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    print(f"Raw rows: {len(df):,}")

    df = remove_sheet_overlap(df)
    print(f"Rows after removing overlap: {len(df):,}")

    day = pd.to_datetime(df["InvoiceDate"]).dt.normalize()

    # Start from empty output folders so re-running gives the same result.
    for sub in ("initial", "daily"):
        shutil.rmtree(out / sub, ignore_errors=True)
        (out / sub).mkdir(parents=True)

    history = df[day < replay_start]
    last_hist = (replay_start - pd.Timedelta(days=1)).date()
    history_file = out / "initial" / f"history_until_{last_hist}.csv"
    history.to_csv(history_file, index=False)
    print(f"Initial history: {len(history):,} rows -> {history_file.relative_to(ROOT)}")

    replay = df[day >= replay_start]
    manifest = []
    for d, rows in replay.groupby(day[day >= replay_start], sort=True):
        name = f"{d.date()}.csv"
        rows.to_csv(out / "daily" / name, index=False)
        manifest.append({"date": d.date(), "file": name, "rows": len(rows)})

    manifest = pd.DataFrame(manifest)
    manifest.to_csv(out / "manifest.csv", index=False)
    print(f"Daily files: {len(manifest)} trading days, {manifest['rows'].sum():,} rows "
          f"({manifest['date'].min()} to {manifest['date'].max()}), "
          f"median {int(manifest['rows'].median()):,} rows/day")


if __name__ == "__main__":
    main()
