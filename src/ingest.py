# src/ingest.py
import uuid, argparse
from datetime import datetime, timedelta, timezone
import duckdb
from src.storage import init_db
from src import config
from src.quota import get_calls_this_month, record_call, check_monthly_cap
from src.ingestors import (
    generate_indicators, generate_indices, insert_indicators, insert_indices
)
from src.checks import run_all_checks

def ingest_last_30_days():
    run_id = str(uuid.uuid4())
    start = datetime.now(timezone.utc)
    from_date = (start - timedelta(days=30)).date()
    to_date = start.date()

    print(f"Starting run {run_id} for {from_date} -> {to_date}")

    indicators = generate_indicators(from_date, to_date)
    indices = generate_indices(from_date, to_date)

    inserted_indicators_total = insert_indicators(indicators)
    inserted_indices_total = insert_indices(indices)

    # run DQ checks
    run_all_checks(run_id)

    con = duckdb.connect(config.DB_PATH)
    con.execute(
        """
        INSERT INTO ingestion_log
        (run_id, dataset, from_date, to_date, rows_ingested, api_calls, started_at, ended_at, status, error)
        VALUES (?, 'demo', ?, ?, ?, ?, ?, ?, 'SUCCESS', NULL)
        """,
        [
            run_id, from_date, to_date,
            len(indicators) + len(indices),
            0,  # API calls = 0 for demo
            start, datetime.now(timezone.utc),
        ],
    )
    con.close()

    print(f"✅ Inserted {len(indicators)} indicators, {len(indices)} indices")
    print(f"📦 Totals now: indicators_raw={inserted_indicators_total}, indices_raw={inserted_indices_total}")
    print(f"🔎 DQ checks recorded for run_id={run_id}")

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
