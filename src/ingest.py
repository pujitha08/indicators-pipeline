# src/ingest.py
import uuid, argparse
from datetime import datetime, timedelta, timezone
import duckdb
from src.storage import init_db
from src import config
from src.quota import get_calls_this_month, record_call, check_monthly_cap


def ingest_last_30_days():
    run_id = str(uuid.uuid4())
    start = datetime.now(timezone.utc)
    from_date = (start - timedelta(days=30)).date()
    to_date = start.date()

    print(f"Starting run {run_id} for {from_date} -> {to_date}")

    # placeholder log entry (we'll insert real data later)
    con = duckdb.connect(config.DB_PATH)
    con.execute(
        """
        INSERT INTO ingestion_log VALUES (?, 'init', ?, ?, 0, 0, ?, ?, 'SUCCESS', NULL)
        """,
        [run_id, from_date, to_date, start, datetime.now(timezone.utc)],
    )
    con.close()

    print(f"Run logged with run_id={run_id}")


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
