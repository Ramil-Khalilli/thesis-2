"""Pipeline monitor: revenue and forecast, data quality, sales and pipeline runs.

Run from the project folder:
    .venv\\Scripts\\streamlit run dashboard\\app.py

The dashboard only reads the warehouse. DuckDB allows one writer OR several readers at a time, so every
query opens a read-only connection, reads and closes it at once; while the pipeline is writing, queries
retry for a few seconds and the page then asks to refresh.
"""

import time
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"

# Colours (reference palette): categorical slots in fixed order, ink for text and the actual series.
INK, INK_2, RULE = "#0b0b0b", "#52514e", "#c3c2b7"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"

st.set_page_config(page_title="Retail pipeline monitor", layout="wide")


class WarehouseBusy(Exception):
    pass


@st.cache_data(ttl=20, show_spinner=False)
def query(sql: str) -> pd.DataFrame:
    last_error = None
    for _ in range(5):
        try:
            con = duckdb.connect(str(WAREHOUSE), read_only=True)
            try:
                return con.execute(sql).df()
            finally:
                con.close()
        except duckdb.CatalogException:
            return pd.DataFrame()          # table not created yet (pipeline step not run)
        except duckdb.IOException as e:    # the pipeline holds the write lock
            last_error = e
            time.sleep(1)
    raise WarehouseBusy(str(last_error))


def money(value) -> str:
    return "–" if value is None or pd.isna(value) else f"£{value:,.0f}"


# ---------------------------------------------------------------- chart helpers

def line_chart(df: pd.DataFrame, x: str, y: str, series: str, colors: dict, dashed=(), y_title="",
               height=320, y_format="~s") -> alt.Chart:
    """Thin lines, legend on top, crosshair with one tooltip listing every series at that date."""
    names = list(colors)
    scale = alt.Scale(domain=names, range=list(colors.values()))
    dash = alt.Scale(domain=names, range=[[5, 4] if n in dashed else [1, 0] for n in names])
    base = alt.Chart(df).encode(x=alt.X(f"{x}:T", title=None))
    lines = base.mark_line(strokeWidth=2).encode(
        y=alt.Y(f"{y}:Q", title=y_title, axis=alt.Axis(format=y_format)),
        color=alt.Color(f"{series}:N", scale=scale, legend=alt.Legend(orient="top", title=None)),
        strokeDash=alt.StrokeDash(f"{series}:N", scale=dash, legend=None),
    )
    nearest = alt.selection_point(nearest=True, on="pointerover", fields=[x], empty=False)
    rule = (
        alt.Chart(df)
        .transform_pivot(series, value=y, groupby=[x])
        .mark_rule(color=RULE, strokeWidth=1)
        .encode(
            x=f"{x}:T",
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip(f"{x}:T", title="Date")]
                    + [alt.Tooltip(f"{n}:Q", format=",.0f") for n in names],
        )
        .add_params(nearest)
    )
    points = base.mark_point(size=64, filled=True).encode(
        y=f"{y}:Q",
        color=alt.Color(f"{series}:N", scale=scale, legend=None),
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
    ).transform_filter(nearest)
    return (lines + rule + points).properties(height=height)


def bar_chart(df: pd.DataFrame, x: str, y: str, color=BLUE, horizontal=False, height=320,
              value_format=",.0f", x_title="", y_title="", tooltip_extra=()) -> alt.Chart:
    if horizontal:
        enc = dict(y=alt.Y(f"{x}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=280)),
                   x=alt.X(f"{y}:Q", title=y_title, axis=alt.Axis(format="~s")))
        mark = alt.Chart(df).mark_bar(color=color, cornerRadiusEnd=4, height={"band": 0.7})
    else:
        enc = dict(x=alt.X(x, title=x_title),
                   y=alt.Y(f"{y}:Q", title=y_title, axis=alt.Axis(format="~s")))
        mark = alt.Chart(df).mark_bar(color=color, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    tooltip = [alt.Tooltip(x), alt.Tooltip(f"{y}:Q", format=value_format)] + list(tooltip_extra)
    return mark.encode(**enc, tooltip=tooltip).properties(height=height)


def show(chart):
    st.altair_chart(chart, width="stretch")


def table(df: pd.DataFrame, **kwargs):
    df = df.copy()
    for col in df.select_dtypes("datetime").columns:   # show pure dates without 00:00:00
        if (df[col].dropna().dt.normalize() == df[col].dropna()).all():
            df[col] = df[col].dt.date
    st.dataframe(df, hide_index=True, width="stretch", **kwargs)


def day(value) -> str:
    return f"{pd.Timestamp(value):%d %b %Y}"


# ---------------------------------------------------------------- page

head, refresh = st.columns([5, 1])
head.title("Online Retail II – pipeline monitor")
if refresh.button("Refresh", width="stretch"):
    st.cache_data.clear()
    st.rerun()

if not WAREHOUSE.exists():
    st.info("No warehouse yet. Run `.venv\\Scripts\\python -m pipeline.flows bootstrap` first.")
    st.stop()

try:
    progress = query("""
        SELECT max(batch_date) AS last_batch, count(*) AS batches, sum(rows_silver) AS silver_rows
        FROM silver.batch_quality
    """)
    accuracy = query("""
        SELECT count(*) AS n, avg(model_abs_error) AS model_mae, avg(baseline_abs_error) AS baseline_mae
        FROM ml.forecast_accuracy WHERE actual IS NOT NULL
    """)
    next_fc = query("""
        SELECT target_date, prediction, model_version FROM ml.forecasts
        ORDER BY made_after DESC, created_at DESC LIMIT 1
    """)
    st.caption(f"Data up to batch **{day(progress.last_batch[0])}** · warehouse read at {time.strftime('%H:%M:%S')}")

    tab_overview, tab_forecast, tab_quality, tab_sales, tab_runs = st.tabs(
        ["Overview", "Forecast", "Data quality", "Sales", "Pipeline runs"])

    # ------------------------------------------------------------ overview
    with tab_overview:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest batch", day(progress.last_batch[0]),
                  f"{progress.batches[0]} trading days in the warehouse", delta_color="off", delta_arrow="off")
        c2.metric("Clean invoice lines (silver)", f"{progress.silver_rows[0]:,.0f}")
        if not accuracy.empty and accuracy.n[0] > 0:
            gain = 1 - accuracy.model_mae[0] / accuracy.baseline_mae[0]
            c3.metric("Forecast error (MAE)", money(accuracy.model_mae[0]),
                      f"{gain:.0%} lower than baseline over {accuracy.n[0]} days",
                      delta_color="normal" if gain > 0 else "inverse")
        else:
            c3.metric("Forecast error (MAE)", "–", "no scored forecasts yet", delta_color="off", delta_arrow="off")
        if not next_fc.empty:
            c4.metric(f"Next forecast · {next_fc.target_date[0]:%a %d %b %Y}", money(next_fc.prediction[0]),
                      f"model v{next_fc.model_version[0]}", delta_color="off", delta_arrow="off")

        st.subheader("Daily net product revenue")
        window = st.radio("Period", ["Last 120 days", "All"], horizontal=True, label_visibility="collapsed")
        revenue = query("""
            SELECT s.invoice_date AS date, s.net_product_revenue::DOUBLE AS revenue,
                   f.prediction AS forecast
            FROM gold.daily_sales s
            LEFT JOIN (SELECT target_date, prediction FROM ml.forecast_accuracy) f ON f.target_date = s.invoice_date
            ORDER BY 1
        """)
        if window == "Last 120 days" and not revenue.empty:
            revenue = revenue[revenue.date >= revenue.date.max() - pd.Timedelta(days=120)]
        long = revenue.melt("date", ["revenue", "forecast"], "series", "value").dropna()
        long["series"] = long["series"].map({"revenue": "Actual", "forecast": "Model forecast"})
        show(line_chart(long, "date", "value", "series", {"Actual": INK, "Model forecast": BLUE},
                        y_title="£ per day"))
        st.caption("Shop trades Sunday to Friday; gaps are Saturdays, bank holidays and the Christmas break.")

    # ------------------------------------------------------------ forecast
    with tab_forecast:
        fa = query("""
            SELECT target_date AS date, actual, prediction, baseline, model_version,
                   model_abs_error, baseline_abs_error
            FROM ml.forecast_accuracy ORDER BY target_date
        """)
        if fa.empty:
            st.info("No forecasts yet.")
        else:
            st.subheader("Forecast vs actual")
            long = fa.melt("date", ["actual", "prediction", "baseline"], "series", "value").dropna()
            long["series"] = long["series"].map(
                {"actual": "Actual", "prediction": "Model forecast", "baseline": "Baseline"})
            show(line_chart(long, "date", "value", "series",
                            {"Actual": INK, "Model forecast": BLUE, "Baseline": ORANGE},
                            dashed=("Baseline",), y_title="£ per day"))
            st.caption("Baseline = revenue on the same weekday one week earlier. "
                       "Each forecast is made after the previous day's batch, using only data up to that day.")

            scored = fa.dropna(subset=["actual"])
            if not scored.empty:
                st.subheader("Average error by month")
                monthly = (scored.assign(month=pd.to_datetime(scored.date).dt.strftime("%Y-%m"))
                           .groupby("month")[["model_abs_error", "baseline_abs_error"]].mean().reset_index()
                           .melt("month", var_name="series", value_name="mae"))
                monthly["series"] = monthly["series"].map(
                    {"model_abs_error": "Model", "baseline_abs_error": "Baseline"})
                bars = alt.Chart(monthly).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4,
                                                   stroke="white", strokeWidth=2).encode(
                    x=alt.X("month:N", title=None),
                    xOffset=alt.XOffset("series:N", sort=["Model", "Baseline"]),
                    y=alt.Y("mae:Q", title="Mean absolute error (£)", axis=alt.Axis(format="~s")),
                    color=alt.Color("series:N", scale=alt.Scale(domain=["Model", "Baseline"], range=[BLUE, ORANGE]),
                                    legend=alt.Legend(orient="top", title=None)),
                    tooltip=["month", "series", alt.Tooltip("mae:Q", title="MAE", format=",.0f")],
                ).properties(height=300)
                show(bars)

            st.subheader("Model versions and retraining decisions")
            table(query("""
                SELECT as_of_date AS "as of", event, champion_version AS champion,
                       round(challenger_mae) AS "challenger MAE", round(champion_mae) AS "champion MAE",
                       round(baseline_mae) AS "baseline MAE", message
                FROM ml.model_events ORDER BY event_time DESC
            """))
            st.subheader("Latest forecasts")
            latest = fa.sort_values("date", ascending=False).head(15).copy()
            latest["error"] = latest["prediction"] - latest["actual"]
            latest = latest.rename(columns={"prediction": "forecast (£)", "actual": "actual (£)",
                                            "error": "forecast − actual (£)", "baseline": "baseline (£)",
                                            "model_version": "model version"})
            money_cols = ["forecast (£)", "actual (£)", "forecast − actual (£)", "baseline (£)"]
            table(latest[["date"] + money_cols + ["model version"]],
                  column_config={c: st.column_config.NumberColumn(format="localized") for c in money_cols})

    # ------------------------------------------------------------ data quality
    with tab_quality:
        log = query("SELECT status, count(*) AS files FROM meta.ingestion_log GROUP BY 1 ORDER BY 2 DESC")
        totals = query("""
            SELECT sum(rows_bronze) AS bronze, sum(duplicates_dropped) AS dups, sum(rows_quarantined) AS quarantined,
                   sum(missing_customer_lines) AS no_customer, sum(rows_silver) AS silver
            FROM silver.batch_quality
        """)
        files = dict(zip(log.status, log.files)) if not log.empty else {}
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows received (bronze)", f"{totals.bronze[0]:,.0f}")
        c2.metric("Duplicate lines dropped", f"{totals.dups[0]:,.0f}",
                  f"{totals.dups[0] / totals.bronze[0]:.1%} of rows", delta_color="off")
        c3.metric("Rows quarantined", f"{totals.quarantined[0]:,.0f}")
        c4.metric("Files rejected or skipped",
                  f"{files.get('rejected', 0) + files.get('skipped_duplicate', 0):,}",
                  f"{files.get('rejected', 0)} rejected · {files.get('skipped_duplicate', 0)} re-sent",
                  delta_color="off")

        quality = query("""
            SELECT batch_date AS date, duplicates_dropped, rows_quarantined,
                   missing_customer_lines::DOUBLE / nullif(rows_silver, 0) AS missing_customer_share
            FROM silver.batch_quality ORDER BY 1
        """)
        left, right = st.columns(2)
        with left:
            st.subheader("Duplicate lines dropped per batch")
            show(bar_chart(quality, "date:T", "duplicates_dropped", height=260, y_title="lines"))
        with right:
            st.subheader("Lines without a customer ID (7-day average)")
            share = quality.assign(
                series="Share without customer ID",
                missing_customer_share=quality["missing_customer_share"].rolling(7, min_periods=1).mean())
            show(line_chart(share, "date", "missing_customer_share", "series",
                            {"Share without customer ID": BLUE}, height=260, y_format="%")
                 .configure_legend(disable=True))

        left, right = st.columns(2)
        with left:
            st.subheader("Files by ingestion status")
            table(log)
            st.subheader("Recent file events other than normal loads")
            table(query("""
                SELECT finished_at AS time, file_name, batch_date, status, message
                FROM meta.ingestion_log WHERE status <> 'loaded' ORDER BY finished_at DESC LIMIT 20
            """))
        with right:
            st.subheader("Quarantined rows by reason")
            table(query("SELECT dq_reason AS reason, count(*) AS rows FROM silver.quarantine_lines GROUP BY 1 ORDER BY 2 DESC"))
            st.subheader("Latest quarantined rows")
            table(query("""
                SELECT batch_date, dq_reason, invoice, stock_code, description, raw_quantity, raw_price
                FROM silver.quarantine_lines ORDER BY batch_date DESC LIMIT 20
            """))

    # ------------------------------------------------------------ sales
    with tab_sales:
        countries = query("""
            SELECT country, sum(net_product_revenue)::DOUBLE AS revenue
            FROM gold.daily_sales_by_country GROUP BY 1 ORDER BY 2 DESC
        """)
        uk_share = countries.loc[countries.country == "United Kingdom", "revenue"].sum() / countries.revenue.sum()
        st.metric("United Kingdom share of net product revenue", f"{uk_share:.1%}")
        left, right = st.columns(2)
        with left:
            st.subheader("Top 10 countries outside the UK")
            show(bar_chart(countries[countries.country != "United Kingdom"].head(10), "country", "revenue",
                           horizontal=True, y_title="net revenue (£)"))
        with right:
            st.subheader("Top 10 products")
            products = query("""
                SELECT p.description AS product, sum(f.line_amount)::DOUBLE AS revenue, f.stock_code
                FROM gold.fact_sales f
                JOIN gold.dim_product p ON p.stock_code = f.stock_code AND p.is_current
                GROUP BY ALL ORDER BY revenue DESC LIMIT 10
            """)
            show(bar_chart(products, "product", "revenue", horizontal=True, y_title="net revenue (£)",
                           tooltip_extra=[alt.Tooltip("stock_code", title="stock code")]))

    # ------------------------------------------------------------ pipeline runs
    with tab_runs:
        runs = query("""
            SELECT started_at, flow, status, files, batch_dates, rows_loaded,
                   ingest_s, transform_s, forecast_s,
                   date_diff('millisecond', started_at, finished_at) / 1000.0 AS total_s, message
            FROM meta.pipeline_runs ORDER BY started_at DESC
        """)
        if runs.empty:
            st.info("No daily runs recorded yet.")
        else:
            st.subheader("Time per step (latest 30 runs)")
            recent = runs.head(30).copy()
            recent["run_number"] = len(runs) - recent.reset_index(drop=True).index   # 1 = first run ever
            steps = (recent.melt(["started_at", "batch_dates", "run_number"], ["ingest_s", "transform_s", "forecast_s"],
                                 "step", "seconds").dropna())
            step_names = {"ingest_s": "Ingest", "transform_s": "Transform (dbt)", "forecast_s": "Forecast"}
            steps["order"] = steps["step"].map({k: i for i, k in enumerate(step_names)})
            steps["step"] = steps["step"].map(step_names)
            steps["run"] = "Run " + steps["run_number"].astype(str)
            steps["started"] = steps["started_at"].dt.strftime("%d %b %H:%M:%S")
            show(alt.Chart(steps).mark_bar(stroke="white", strokeWidth=2).encode(
                x=alt.X("run:N", sort=alt.SortField("run_number"), title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("seconds:Q", title="seconds"),
                color=alt.Color("step:N", scale=alt.Scale(domain=list(step_names.values()),
                                                          range=[BLUE, ORANGE, AQUA]),
                                legend=alt.Legend(orient="top", title=None)),
                order=alt.Order("order:Q"),
                tooltip=["run", "started", "step", alt.Tooltip("seconds:Q", format=".1f"),
                         alt.Tooltip("batch_dates:Q", title="days processed")],
            ).properties(height=300))
            st.caption("Times are measured inside the flow. Prefect's start-up before the flow begins "
                       "(about 20 s with its temporary local server) is not included.")
            st.subheader("All runs")
            table(runs)

except WarehouseBusy:
    st.warning("The pipeline is writing to the warehouse right now. Press **Refresh** in a few seconds.")
