"""Next-trading-day revenue forecast: training, MLflow registry, daily prediction, monthly retraining.

Model: ridge regression on the features in features.py, compared against a seasonal naive baseline
(same weekday last week). The production model is the MLflow registered model version with the alias
"champion".

For every new batch date D the pipeline:
  1. (policy "monthly") on the first batch of a new month (at least 28 days after the last check),
     trains a challenger on data up to D-28,
     compares it with the champion on the last 28 days, and promotes it (refitted on all data up to D)
     only if it is more accurate. Every decision is stored in ml.model_events.
  2. predicts the next expected trading day after D and stores it in ml.forecasts.
Actual revenue arrives with a later batch; ml.forecast_accuracy joins forecasts with gold.daily_sales.

Features for a target day only use data from before that day, so processing many days in one run
gives the same forecasts as processing them one by one.
"""

import os
from datetime import date, datetime

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")   # must be set before mlflow is imported

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.impute import SimpleImputer
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from . import config
from .features import FEATURES, baseline, features_for, load_daily, next_trading_day, smape, training_frame
from .warehouse import connect

MODEL_NAME = "daily_revenue_forecast"
ALIAS = "champion"
HOLDOUT_DAYS = 28
EXPERIMENT = "daily-revenue-forecast"
# MLflow saves sklearn models with skops, which only reloads types listed as trusted at save time.
SKOPS_TRUSTED = ["numpy.dtype"]


def retrain_policy() -> str:
    """'monthly' (default) or 'never'; set with the THESIS_RETRAIN_POLICY environment variable."""
    return os.environ.get("THESIS_RETRAIN_POLICY", "monthly")


# ---------------------------------------------------------------- setup

def setup_mlflow() -> MlflowClient:
    uri = f"sqlite:///{(config.DATA / 'mlflow.db').as_posix()}"
    mlflow.set_tracking_uri(uri)
    mlflow.set_registry_uri(uri)
    if mlflow.get_experiment_by_name(EXPERIMENT) is None:
        artifacts = config.DATA / "mlartifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        mlflow.create_experiment(EXPERIMENT, artifact_location=artifacts.as_uri())
    mlflow.set_experiment(EXPERIMENT)
    return MlflowClient()


def ensure_tables(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ml")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ml.forecasts (
            target_date   DATE,
            made_after    DATE,       -- last batch date the forecast could use
            model_version INTEGER,
            prediction    DOUBLE,
            baseline      DOUBLE,     -- same weekday last week
            created_at    TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ml.model_events (
            event_time          TIMESTAMP,
            as_of_date          DATE,
            event               VARCHAR,   -- initial_model | promoted | kept_champion
            champion_version    INTEGER,   -- champion after the event
            challenger_mae      DOUBLE,    -- on the last 28 days, model trained without them
            champion_mae        DOUBLE,
            baseline_mae        DOUBLE,
            message             VARCHAR
        )
    """)
    con.execute("""
        CREATE OR REPLACE VIEW ml.forecast_accuracy AS
        SELECT f.target_date, f.made_after, f.model_version, f.prediction, f.baseline,
               s.net_product_revenue::DOUBLE                      AS actual,
               abs(f.prediction - s.net_product_revenue::DOUBLE)  AS model_abs_error,
               abs(f.baseline   - s.net_product_revenue::DOUBLE)  AS baseline_abs_error
        FROM ml.forecasts f
        LEFT JOIN gold.daily_sales s ON s.invoice_date = f.target_date
        QUALIFY row_number() OVER (PARTITION BY f.target_date ORDER BY f.created_at DESC) = 1
    """)


# ---------------------------------------------------------------- training

def new_model():
    return make_pipeline(SimpleImputer(), StandardScaler(), RidgeCV(alphas=np.logspace(-2, 3, 20)))


def _fit(frame: pd.DataFrame):
    return new_model().fit(frame[FEATURES], frame["actual"])


def _register(client: MlflowClient, model, frame: pd.DataFrame, cutoff: date, metrics: dict, reason: str) -> int:
    with mlflow.start_run(run_name=f"{reason}-{cutoff}"):
        mlflow.log_params({"model": "ridge", "features": len(FEATURES), "cutoff": str(cutoff),
                           "train_rows": len(frame), "holdout_days": HOLDOUT_DAYS, "reason": reason})
        mlflow.log_metrics({k: v for k, v in metrics.items() if v is not None})
        info = mlflow.sklearn.log_model(
            sk_model=model, name="model", registered_model_name=MODEL_NAME,
            skops_trusted_types=SKOPS_TRUSTED,
            signature=infer_signature(frame[FEATURES], model.predict(frame[FEATURES])),
        )
    version = int(info.registered_model_version)
    client.set_model_version_tag(MODEL_NAME, str(version), "cutoff", str(cutoff))
    client.set_registered_model_alias(MODEL_NAME, ALIAS, str(version))
    return version


def champion(client: MlflowClient):
    """(version, last checked date, model) of the current champion, or None.

    'last checked' is the training cutoff, or the date of the latest monthly check it survived.
    """
    try:
        mv = client.get_model_version_by_alias(MODEL_NAME, ALIAS)
    except Exception:
        return None
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{ALIAS}")
    checked = mv.tags.get("last_checked", mv.tags["cutoff"])
    return int(mv.version), date.fromisoformat(checked), model


def _holdout(daily: pd.DataFrame, as_of: date):
    """Training rows up to as_of minus the holdout window, and the holdout rows themselves."""
    split = pd.Timestamp(as_of) - pd.Timedelta(days=HOLDOUT_DAYS)
    frame = training_frame(daily, last_target=as_of)
    return frame, frame[frame["target_date"] <= split], frame[frame["target_date"] > split]


def train_initial(con, client: MlflowClient, as_of: date) -> int:
    daily = load_daily(con)
    frame, train, test = _holdout(daily, as_of)
    candidate = _fit(train)
    mae = mean_absolute_error(test["actual"], candidate.predict(test[FEATURES]))
    base_mae = mean_absolute_error(test["actual"], baseline(test))
    model = _fit(frame)
    version = _register(client, model, frame, as_of,
                        {"holdout_mae": mae, "holdout_baseline_mae": base_mae,
                         "holdout_smape": smape(test["actual"], candidate.predict(test[FEATURES]))}, "initial")
    con.execute("INSERT INTO ml.model_events VALUES (?, ?, 'initial_model', ?, ?, NULL, ?, ?)",
                [datetime.now(), as_of, version, mae, base_mae, f"trained on {len(frame)} days"])
    return version


def challenge(con, client: MlflowClient, as_of: date, current) -> int:
    """Monthly retraining: promote a new model only if it beats the champion on the last 28 days."""
    daily = load_daily(con)
    frame, train, test = _holdout(daily, as_of)
    version, _, champ_model = current
    challenger_mae = mean_absolute_error(test["actual"], _fit(train).predict(test[FEATURES]))
    champion_mae = mean_absolute_error(test["actual"], champ_model.predict(test[FEATURES]))
    base_mae = mean_absolute_error(test["actual"], baseline(test))

    if challenger_mae < champion_mae:
        model = _fit(frame)
        version = _register(client, model, frame, as_of,
                            {"holdout_mae": challenger_mae, "holdout_baseline_mae": base_mae,
                             "champion_holdout_mae": champion_mae}, "monthly")
        event, message = "promoted", f"challenger better by {champion_mae - challenger_mae:,.0f} MAE"
    else:
        client.set_model_version_tag(MODEL_NAME, str(version), "last_checked", str(as_of))
        event, message = "kept_champion", f"challenger worse by {challenger_mae - champion_mae:,.0f} MAE"
    con.execute("INSERT INTO ml.model_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [datetime.now(), as_of, event, version, challenger_mae, champion_mae, base_mae, message])
    return version


# ---------------------------------------------------------------- daily step

def forecast_after(batch_dates: list[str] | None = None) -> list[str]:
    """Retrain if due and forecast the next trading day, for every new batch date in order.

    batch_dates None = only the latest date in gold. Dates not newer than the last forecast are
    skipped, so re-running or correcting an old day never rewrites past forecasts.
    """
    client = setup_mlflow()
    con = connect()
    log = []
    try:
        ensure_tables(con)
        latest_gold = con.execute("SELECT max(invoice_date) FROM gold.daily_sales").fetchone()[0]
        dates = sorted({date.fromisoformat(str(d)) for d in batch_dates}) if batch_dates else [latest_gold]
        last_done = con.execute("SELECT max(made_after) FROM ml.forecasts").fetchone()[0]
        dates = [d for d in dates if last_done is None or d > last_done]

        current = champion(client)
        if current is None:
            as_of = dates[0] if dates else latest_gold
            train_initial(con, client, as_of)
            current = champion(client)
            log.append(f"initial model v{current[0]} trained on data up to {as_of}")

        daily = load_daily(con)
        for d in dates:
            version, checked, _ = current
            # First batch of a new month, and the 28-day comparison window lies after the last check,
            # so the champion is never judged on days it was trained on.
            due = (d.year, d.month) > (checked.year, checked.month) and (d - checked).days >= HOLDOUT_DAYS
            if retrain_policy() == "monthly" and due:
                new_version = challenge(con, client, d, current)
                current = champion(client)
                log.append(f"{d}: monthly check -> "
                           + (f"promoted v{new_version}" if new_version != version else f"kept v{version}"))

            target = next_trading_day(d, daily)
            if target is None:
                continue
            row = features_for(pd.Timestamp(target), daily)
            frame = pd.DataFrame([row])
            prediction = float(current[2].predict(frame[FEATURES])[0])
            base = float(baseline(frame).iloc[0])
            con.execute("DELETE FROM ml.forecasts WHERE target_date = ? AND model_version = ?",
                        [target, current[0]])
            con.execute("INSERT INTO ml.forecasts VALUES (?, ?, ?, ?, ?, ?)",
                        [target, d, current[0], prediction, base, datetime.now()])
            log.append(f"forecast for {target} (after {d}, model v{current[0]}): "
                       f"£{prediction:,.0f}, baseline £{base:,.0f}")
    finally:
        con.close()
    return log


def evaluation_summary() -> str:
    con = connect()
    try:
        ensure_tables(con)
        overall = con.execute("""
            SELECT count(*) AS days, round(avg(model_abs_error)) AS model_mae,
                   round(avg(baseline_abs_error)) AS baseline_mae,
                   round(100 * (1 - avg(model_abs_error) / avg(baseline_abs_error)), 1) AS improvement_pct
            FROM ml.forecast_accuracy WHERE actual IS NOT NULL
        """).df()
        monthly = con.execute("""
            SELECT strftime(target_date, '%Y-%m') AS month, count(*) AS days,
                   round(avg(model_abs_error)) AS model_mae, round(avg(baseline_abs_error)) AS baseline_mae,
                   string_agg(DISTINCT 'v' || model_version, ',') AS versions
            FROM ml.forecast_accuracy WHERE actual IS NOT NULL GROUP BY 1 ORDER BY 1
        """).df()
        events = con.execute("""
            SELECT as_of_date, event, champion_version AS champion, round(challenger_mae) AS challenger_mae,
                   round(champion_mae) AS champion_mae, round(baseline_mae) AS baseline_mae, message
            FROM ml.model_events ORDER BY event_time
        """).df()
    finally:
        con.close()
    return (f"Forecasts scored so far:\n{overall.to_string(index=False)}\n\n"
            f"By month:\n{monthly.to_string(index=False)}\n\n"
            f"Model events:\n{events.to_string(index=False)}")
