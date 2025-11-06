# src/status_app.py
import streamlit as st
import duckdb
from datetime import date, timedelta
from src import config

st.set_page_config(page_title="Pipeline Status", layout="wide")

@st.cache_data(ttl=30)
def load_data():
    con = duckdb.connect(config.DB_PATH, read_only=True)

    last_run = con.execute(
        """
        SELECT *
        FROM ingestion_log
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchdf()

    dq = con.execute(
        """
        WITH latest AS (
          SELECT run_id
          FROM ingestion_log
          ORDER BY started_at DESC
          LIMIT 1
        )
        SELECT d.dataset, d.check_name, d.status, d.observed_value, d.threshold, d.checked_at
        FROM dq_results d
        JOIN latest l USING (run_id)
        ORDER BY d.dataset, d.check_name
        """
    ).fetchdf()

    counts = con.execute(
        """
        SELECT 'indicators_raw' AS table, COUNT(*) AS rows FROM indicators_raw
        UNION ALL
        SELECT 'indices_raw' AS table, COUNT(*) AS rows FROM indices_raw
        """
    ).fetchdf()

    counts_30d = con.execute(
        """
        SELECT 'indicators_30d' AS table, COUNT(*) AS rows
          FROM indicators_raw WHERE as_of_date >= CURRENT_DATE - INTERVAL 30 DAY
        UNION ALL
        SELECT 'indices_30d' AS table, COUNT(*) AS rows
          FROM indices_raw WHERE as_of_date >= CURRENT_DATE - INTERVAL 30 DAY
        """
    ).fetchdf()

    quota = con.execute(
        """
        SELECT COALESCE(SUM(calls),0) AS calls_this_month
        FROM api_quota
        WHERE month_yyyymm = strftime(CURRENT_DATE, '%Y%m')
        """
    ).fetchdf()

    # latest dates for freshness display
    max_dates = con.execute(
        """
        SELECT
          (SELECT MAX(as_of_date) FROM indicators_raw) AS indicators_latest,
          (SELECT MAX(as_of_date) FROM indices_raw) AS indices_latest
        """
    ).fetchdf()

    con.close()
    return last_run, dq, counts, counts_30d, quota, max_dates

# ------------- UI -------------
st.title("Indicators/Indices Pipeline — Status")

last_run, dq, counts, counts_30d, quota, max_dates = load_data()

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Last Run")
    if len(last_run):
        r = last_run.iloc[0]
        st.write(f"**Run ID:** `{r.run_id}`")
        st.write(f"**Window:** {r.from_date} → {r.to_date}")
        st.write(f"**Rows ingested:** {int(r.rows_ingested)}")
        st.write(f"**API calls:** {int(r.api_calls)}")
        st.write(f"**Started:** {r.started_at}")
        st.write(f"**Ended:** {r.ended_at}")
        st.success("SUCCESS" if r.status == "SUCCESS" else r.status)
    else:
        st.warning("No runs yet.")

with col2:
    st.subheader("Row Counts (total)")
    st.dataframe(counts, use_container_width=True, hide_index=True)
    st.subheader("Row Counts (last 30 days)")
    st.dataframe(counts_30d, use_container_width=True, hide_index=True)

with col3:
    st.subheader("API Quota")
    calls = int(quota.iloc[0].calls_this_month) if len(quota) else 0
    cap = config.MONTHLY_CAP
    st.metric("Calls this month", f"{calls} / {cap}")
    st.progress(min(calls / cap, 1.0))
    st.caption("Cap: MONTHLY_CAP from .env")

st.divider()

c1, c2 = st.columns(2)
with c1:
    st.subheader("Indicators — Freshness/Nulls/Dupes")
    st.dataframe(dq[dq["dataset"] == "indicators"][["check_name", "status", "observed_value", "threshold", "checked_at"]],
                 use_container_width=True, hide_index=True)

with c2:
    st.subheader("Indices — Freshness/Nulls/Dupes")
    st.dataframe(dq[dq["dataset"] == "indices"][["check_name", "status", "observed_value", "threshold", "checked_at"]],
                 use_container_width=True, hide_index=True)

st.divider()

# friendly badges for latest dates
id_latest = max_dates["indicators_latest"].iloc[0]
ix_latest = max_dates["indices_latest"].iloc[0]
st.caption(
    f"Latest dates — Indicators: **{id_latest}**, Indices: **{ix_latest}**  •  Today: **{date.today()}**"
)

st.caption("Tip: refresh after running `python -m src.ingest` for new results.")
