# src/quota.py
from datetime import datetime, timezone
import duckdb
from src import config

def _yyyymm(dt: datetime) -> str:
    return dt.strftime("%Y%m")

def get_calls_this_month() -> int:
    now = datetime.now(timezone.utc)
    yyyymm = _yyyymm(now)
    con = duckdb.connect(config.DB_PATH)
    rows = con.execute(
        "SELECT calls FROM api_quota WHERE month_yyyymm = ?",
        [yyyymm]
    ).fetchall()
    con.close()
    return rows[0][0] if rows else 0

def record_call(n: int = 1) -> int:
    now = datetime.now(timezone.utc)
    yyyymm = _yyyymm(now)
    con = duckdb.connect(config.DB_PATH)
    con.execute("""
        INSERT INTO api_quota(month_yyyymm, calls)
        VALUES (?, ?)
        ON CONFLICT (month_yyyymm)
        DO UPDATE SET calls = api_quota.calls + EXCLUDED.calls
    """, [yyyymm, n])
    calls = con.execute(
        "SELECT calls FROM api_quota WHERE month_yyyymm = ?",
        [yyyymm]
    ).fetchone()[0]
    con.close()
    return calls

def check_monthly_cap() -> None:
    calls = get_calls_this_month()
    if calls >= config.MONTHLY_CAP:
        raise RuntimeError(
            f"Monthly API cap reached: {calls}/{config.MONTHLY_CAP}"
        )
