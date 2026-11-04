"""Features for the next-trading-day revenue forecast.

Every feature for a target day T uses only data from days before T, so the same code builds
the training set (from history) and the live prediction (after the latest batch).
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd

FEATURES = [
    "day_of_week", "month", "day_of_month", "days_to_christmas",
    "rev_last_day", "rev_same_weekday_last_week", "rev_same_weekday_mean_4w",
    "rev_mean_5d", "rev_mean_20d", "rev_std_20d",
    "invoices_mean_5d", "customers_mean_5d",
]
MIN_HISTORY_DAYS = 28   # a target needs at least this much history before it


def load_daily(con) -> pd.DataFrame:
    """Calendar with daily KPIs (NaN on days without trading)."""
    df = con.execute("""
        SELECT d.date_day, d.is_uk_bank_holiday,
               s.net_product_revenue::DOUBLE AS revenue, s.invoices, s.customers
        FROM gold.dim_date d
        LEFT JOIN gold.daily_sales s ON s.invoice_date = d.date_day
        ORDER BY d.date_day
    """).df()
    df["date_day"] = pd.to_datetime(df["date_day"])
    return df.set_index("date_day")


def is_expected_trading_day(day: date, holidays: set) -> bool:
    """The shop trades Sunday to Friday, closes on UK bank holidays and from 24 Dec to 3 Jan."""
    if day.isoweekday() == 6 or day in holidays:
        return False
    if (day.month == 12 and day.day >= 24) or (day.month == 1 and day.day <= 3):
        return False
    return True


def next_trading_day(after: date, daily: pd.DataFrame) -> date | None:
    holidays = set(daily.index[daily["is_uk_bank_holiday"]].date)
    day = after + timedelta(days=1)
    last = daily.index.max().date()
    while day <= last:
        if is_expected_trading_day(day, holidays):
            return day
        day += timedelta(days=1)
    return None


def features_for(target: pd.Timestamp, daily: pd.DataFrame) -> dict:
    past = daily[daily.index < target]
    trading = past.dropna(subset=["revenue"])
    rev = daily["revenue"]

    def rev_at(days_back: int) -> float:
        day = target - pd.Timedelta(days=days_back)
        return rev.get(day, np.nan) if day < target else np.nan

    same_weekday = [rev_at(7 * k) for k in range(1, 5)]
    last5, last20 = trading.tail(5), trading.tail(20)
    christmas = pd.Timestamp(target.year, 12, 25)
    return {
        "target_date": target,
        "day_of_week": target.isoweekday(),
        "month": target.month,
        "day_of_month": target.day,
        "days_to_christmas": (christmas - target).days,
        "rev_last_day": trading["revenue"].iloc[-1] if len(trading) else np.nan,
        "rev_same_weekday_last_week": same_weekday[0],
        "rev_same_weekday_mean_4w": np.nanmean(same_weekday) if not np.all(np.isnan(same_weekday)) else np.nan,
        "rev_mean_5d": last5["revenue"].mean(),
        "rev_mean_20d": last20["revenue"].mean(),
        "rev_std_20d": last20["revenue"].std(),
        "invoices_mean_5d": last5["invoices"].mean(),
        "customers_mean_5d": last5["customers"].mean(),
        "history_days": len(trading),
    }


def training_frame(daily: pd.DataFrame, first_target=None, last_target=None) -> pd.DataFrame:
    """One row per trading day in [first_target, last_target] that has enough history: features + actual."""
    targets = daily.dropna(subset=["revenue"]).index
    if first_target is not None:
        targets = targets[targets >= pd.Timestamp(first_target)]
    if last_target is not None:
        targets = targets[targets <= pd.Timestamp(last_target)]
    rows = [features_for(t, daily) for t in targets]
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame[frame["history_days"] >= MIN_HISTORY_DAYS].copy()
    frame["actual"] = daily.loc[frame["target_date"], "revenue"].to_numpy()
    return frame


def baseline(frame: pd.DataFrame) -> pd.Series:
    """Seasonal naive: same weekday last week, or the 5-day mean when that day had no trading."""
    return frame["rev_same_weekday_last_week"].fillna(frame["rev_mean_5d"])


def smape(actual, predicted) -> float:
    actual, predicted = np.asarray(actual, float), np.asarray(predicted, float)
    denom = (np.abs(actual) + np.abs(predicted)) / 2
    return float(np.mean(np.where(denom == 0, 0, np.abs(actual - predicted) / denom)) * 100)
