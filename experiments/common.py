"""Shared helpers for the experiments.

Each experiment configuration runs in a child process with its own data folder
(experiments/work/<name>/), set through THESIS_DATA_DIR before the pipeline is imported.
The main warehouse in data/ is never touched. Source files are shared (read-only).
"""

import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / "experiments" / "work"
RESULTS = ROOT / "experiments" / "results"
SOURCE_DAILY = ROOT / "data" / "source" / "daily"


def run_child(script: Path, name: str, args: list[str] = (), env_extra: dict | None = None) -> dict:
    """Run `script --child ...` in a fresh workspace and return the JSON it saves."""
    workspace = WORK / name
    shutil.rmtree(workspace, ignore_errors=True)
    workspace.mkdir(parents=True)
    env = {**os.environ, "THESIS_DATA_DIR": str(workspace), "PYTHONIOENCODING": "utf-8", **(env_extra or {})}
    print(f"--- running {name} ...", flush=True)
    proc = subprocess.run([sys.executable, str(script), "--child", *args], env=env, cwd=ROOT,
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    result_file = workspace / "result.json"
    if proc.returncode != 0 or not result_file.exists():
        tail = "\n".join(line for line in proc.stdout.splitlines() + proc.stderr.splitlines()
                         if "leaked" not in line and "nanobind" not in line)[-3000:]
        raise SystemExit(f"{name} failed (exit {proc.returncode}):\n{tail}")
    return json.loads(result_file.read_text())


def save_result(data: dict) -> None:
    Path(os.environ["THESIS_DATA_DIR"], "result.json").write_text(json.dumps(data, indent=2, default=str))


def bootstrap_quietly() -> None:
    """Load year 1 into bronze and build silver + gold, without Prefect."""
    sys.path.insert(0, str(ROOT))
    from pipeline.ingest import bootstrap_history
    from pipeline.transform import run_dbt
    bootstrap_history()
    run_dbt(None, full_refresh=True)


def read_day(day: str) -> tuple[list[str], list[list[str]]]:
    with open(SOURCE_DAILY / f"{day}.csv", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def write_csv(path: Path, header: list[str], rows: list[list[str]], encoding: str = "utf-8") -> None:
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def markdown_table(rows: list[dict]) -> str:
    if not rows:
        return "(no rows)\n"
    cols = list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(lines) + "\n"


def write_outputs(name: str, rows: list[dict], markdown: str) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    if rows:
        with open(RESULTS / f"{name}.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    (RESULTS / f"{name}.md").write_text(markdown, encoding="utf-8")
    print(markdown)
