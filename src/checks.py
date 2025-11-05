# src/checks.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
import duckdb
from typing import Tuple
from src import config

def _now_utc():
    return datetime.now(timezone.utc)

def _insert_result(con, run_id: str, dataset: str, check_name: str, status: str,
                   observed_value: float | None, threshold: str, details: str):
    con.execute(
        """
        INSERT INTO dq_results(run_id, dataset, check_name, status, observed_value, threshold, details, checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, dataset, check_name, status, observed_value, threshold, details, _now_utc()],
    )

def check_freshness(con, table: str, run_id: str, dataset: str, max_lag_days: int = 1):
    row = con.execute(f"SELECT MAX(as_of_date) FROM {table}").fetchone()
    latest = row[0]
    status = "FAIL"
    if latest is not None:
        # PASS if latest date is within max_lag_days
        status = "PASS" if (datetime.now().date() - latest).days <= max_lag_days else "FAIL"
    _insert_result(con, run_id, dataset, "freshness", status, float(latest.toordinal()) if latest else None,
                   f"latest >= today-{max_lag_days}d", f"latest={latest}")

def check_nulls(con, table: str, run_id: str, dataset: str, column: str):
    row = con.execute(
        f"""
        SELECT SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)
        FROM {table}
        WHERE as_of_date >= CURRENT_DATE - INTERVAL 30 DAY
        """
    ).fetchone()
    nulls = int(row[0] or 0)
    status = "PASS" if nulls == 0 else "FAIL"
    _insert_result(con, run_id, dataset, "nulls_last_30d", status, float(nulls), "0", f"nulls={nulls}")

def check_dupes(con, table: str, run_id: str, dataset: str, key_expr: str):
    row = con.execute(
        f"""
        WITH k AS (
          SELECT {key_expr} AS k, COUNT(*) c
          FROM {table}
          WHERE as_of_date >= CURRENT_DATE - INTERVAL 30 DAY
          GROUP BY 1
        )
        SELECT COALESCE(SUM(CASE WHEN c > 1 THEN c - 1 ELSE 0 END), 0) AS dupes
        FROM k
        """
    ).fetchone()
    dupes = int(row[0] or 0)
    status = "PASS" if dupes == 0 else "FAIL"
    _insert_result(con, run_id, dataset, "dupes_last_30d", status, float(dupes), "0", f"dupes={dupes}")

def run_all_checks(run_id: str):
    con = duckdb.connect(config.DB_PATH)

    # indicators_raw checks
    check_freshness(con, "indicators_raw", run_id, "indicators")
    check_nulls(con, "indicators_raw", run_id, "indicators", "value")
    check_dupes(con, "indicators_raw", run_id, "indicators", "as_of_date || '|' || indicator_code")

    # indices_raw checks
    check_freshness(con, "indices_raw", run_id, "indices")
    check_nulls(con, "indices_raw", run_id, "indices", "value")
    check_dupes(con, "indices_raw", run_id, "indices", "as_of_date || '|' || index_code")

    con.close()
