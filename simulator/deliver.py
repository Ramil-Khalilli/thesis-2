"""Simulate daily data arriving: copy the next day's file into data/landing/.

Each "tick" delivers the next trading day from data/source/daily/. Progress is kept in
data/simulator_state.json, so every run continues where the last one stopped.

Files are written under a temporary name and then renamed, so the pipeline never
picks up a half-written file.

Usage (from the project folder):
    python simulator/deliver.py                    deliver the next day
    python simulator/deliver.py --days 5           deliver the next 5 days at once
    python simulator/deliver.py --days 30 --interval 60
                                                   one day every 60 seconds (a sped-up clock)
    python simulator/deliver.py --status           show progress, deliver nothing
    python simulator/deliver.py --reset            start over and empty the landing folder
"""

import argparse
import json
import shutil
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data" / "source" / "daily"
LANDING = ROOT / "data" / "landing"
STATE = ROOT / "data" / "simulator_state.json"


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"last_delivered": None}


def save_state(state: dict) -> None:
    STATE.write_text(json.dumps(state, indent=2))


def deliver(path: Path) -> None:
    tmp = LANDING / (path.name + ".part")
    shutil.copyfile(path, tmp)
    tmp.replace(LANDING / path.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=1, help="how many days to deliver")
    parser.add_argument("--interval", type=float, default=0, help="seconds to wait between days")
    parser.add_argument("--status", action="store_true", help="show progress only")
    parser.add_argument("--reset", action="store_true", help="start over and empty the landing folder")
    args = parser.parse_args()

    files = sorted(SOURCE.glob("*.csv"))
    if not files:
        raise SystemExit("No daily files found. Run: python simulator/split_days.py")

    LANDING.mkdir(parents=True, exist_ok=True)

    if args.reset:
        for f in LANDING.iterdir():
            if f.is_file():
                f.unlink()
        save_state({"last_delivered": None})
        print("Simulator reset; landing folder emptied.")
        return

    state = load_state()
    last = state["last_delivered"]
    pending = [f for f in files if last is None or f.stem > last]

    if args.status:
        print(f"Last delivered: {last or 'nothing yet'} | delivered {len(files) - len(pending)} "
              f"of {len(files)} days | next: {pending[0].stem if pending else 'none, replay finished'}")
        return

    if not pending:
        print("All days have been delivered. Use --reset to start over.")
        return

    for i, f in enumerate(pending[: args.days]):
        if i > 0 and args.interval > 0:
            time.sleep(args.interval)
        deliver(f)
        state["last_delivered"] = f.stem
        save_state(state)
        print(f"Delivered {f.name} -> data/landing/")


if __name__ == "__main__":
    main()
