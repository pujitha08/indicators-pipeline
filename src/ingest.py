# src/ingest.py
import uuid, argparse
from datetime import datetime, timedelta, timezone
import duckdb
from src.storage import init_db
from src import config
from src.quota import get_calls_this_month, record_call, check_monthly_cap
from src.checks import run_all_checks

# ✅ FRED adapter + client
from src.api_client import APIClient
from src.fred_adapter import fetch_series_data, FRED_SERIES

def ingest_last_30_days():
    run_id = str(uuid.uuid4())
    start = datetime.now(timezone.utc)
    from_date = (start - timedelta(days=30)).date()
    to_date = start.date()

    print(f"Starting FRED ingest {run_id} for {from_date} → {to_date}")
    client = APIClient()

    # --- fetch real series ---
    all_rows = []
    for code, name in FRED_SERIES.items():
        print(f"Fetching {name} ({code}) …")
        series_rows = fetch_series_data(client, code, from_date, to_date)
        print(f" → got {len(series_rows)} rows")
        all_rows.extend(series_rows)

    # --- write to DuckDB (indicators_raw) ---
    con = duckdb.connect(config.DB_PATH)
    codes = list({r["code"] for r in all_rows})
    if codes:
        con = duckdb.connect(config.DB_PATH)
        con.execute(
            """
            DELETE FROM indicators_raw
            WHERE indicator_code IN (
                SELECT * FROM UNNEST(?)
            )
            AND as_of_date BETWEEN ? AND ?
            """,
            [codes, from_date, to_date],
        )
        con.close()
    if all_rows:
        codes = list({r["code"] for r in all_rows})
        data = [(r["as_of_date"], r["code"], r["value"], r["source"]) for r in all_rows]

    # Use a single connection for both DELETE and INSERT
        with duckdb.connect(config.DB_PATH) as con:
        # delete any existing rows for these codes in the window
            con.execute(
            """
            DELETE FROM indicators_raw
            WHERE indicator_code IN (SELECT * FROM UNNEST(?))
              AND as_of_date BETWEEN ? AND ?
            """,
            [codes, from_date, to_date],
        )
        # insert fresh rows
            con.executemany(
            "INSERT INTO indicators_raw(as_of_date, indicator_code, value, source) VALUES (?, ?, ?, ?)",
            data,
        )
    con.close()

    # --- run DQ checks ---
    run_all_checks(run_id)

    # --- log the run ---
    con = duckdb.connect(config.DB_PATH)
    con.execute(
        """
        INSERT INTO ingestion_log
        (run_id, dataset, from_date, to_date, rows_ingested, api_calls, started_at, ended_at, status, error)
        VALUES (?, 'fred', ?, ?, ?, ?, ?, ?, 'SUCCESS', NULL)
        """,
        [
            run_id,
            from_date,
            to_date,
            len(all_rows),
            # (API calls are recorded in the client’s quota ledger already)
            0,
            start,
            datetime.now(timezone.utc),
        ],
    )
    con.close()

    print(f"Ingested {len(all_rows)} total FRED rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo-calls", type=int, default=0, help="simulate spending N API calls")
    args = parser.parse_args()

    init_db()

    if args.demo_calls > 0:
        print(f"Calls before: {get_calls_this_month()}")
        check_monthly_cap()
        record_call(args.demo_calls)
        print(f"Calls after:  {get_calls_this_month()}  (added {args.demo_calls})")
    else:
        ingest_last_30_days()
