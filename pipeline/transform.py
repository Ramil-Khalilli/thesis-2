"""Run the dbt project (transform/) from Python."""

import json
import os

from dbt.cli.main import dbtRunner

from . import config

TRANSFORM = config.ROOT / "transform"


class TransformFailed(RuntimeError):
    pass


def run_dbt(batch_dates: list[str] | None = None, full_refresh: bool = False) -> None:
    """dbt build for the given batch dates (None = all dates). Raises if a model or test fails."""
    os.environ["THESIS_WAREHOUSE"] = config.WAREHOUSE.as_posix()
    args = ["build", "--project-dir", str(TRANSFORM), "--profiles-dir", str(TRANSFORM)]
    if batch_dates:
        args += ["--vars", json.dumps({"batch_dates": sorted(set(batch_dates))})]
    if full_refresh:
        args.append("--full-refresh")

    result = dbtRunner().invoke(args)
    if not result.success:
        raise TransformFailed(f"dbt build failed: {result.exception or 'a model or test failed (see log above)'}")
