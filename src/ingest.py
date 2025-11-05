import uuid
from datetime import datetime, timedelta
import duckdb
from src.storage import init_db
from src import config

def ingest_last_30_days():
    run_id = str(uuid.uuid4())
    start = datetime.utcnow()
    from_date = (start - timedelta(days=30)).date()
    to_date = start.date()

    # TODO: fetch data here later
    print(f"Starting run {run_id} for {from_date} -> {to_date}")

    # for now just log a fake run
    con = duckdb.connect(config.DB_PATH)
    con.execute("""
        INSERT INTO ingestion_log VALUES (?, 'init', ?, ?, 0, 0, ?, ?, 'SUCCESS', NULL)
    """, [run_id, from_date, to_date, start, datetime.utcnow()])
    con.close()

    print(f" Run logged with run_id={run_id}")

if __name__ == "__main__":
    init_db()
    ingest_last_30_days()
