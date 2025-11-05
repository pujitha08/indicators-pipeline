CREATE TABLE IF NOT EXISTS indicators_raw (
  as_of_date DATE,
  indicator_code TEXT,
  value DOUBLE,
  source TEXT,
  _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS indices_raw (
  as_of_date DATE,
  index_code TEXT,
  value DOUBLE,
  source TEXT,
  _ind_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingestion_log (
  run_id TEXT,
  dataset TEXT,
  from_date DATE,
  to_date DATE,
  rows_ingested INTEGER,
  api_calls INTEGER,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status TEXT,
  error TEXT
);

CREATE TABLE IF NOT EXISTS dq_results (
  run_id TEXT,
  dataset TEXT,
  check_name TEXT,
  status TEXT,
  observed_value DOUBLE,
  threshold TEXT,
  details TEXT,
  checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_quota (
  month_yyyymm TEXT PRIMARY KEY,
  calls INTEGER
);

CREATE TABLE IF NOT EXISTS websocket_ticks (
  ts TIMESTAMP,
  symbol TEXT,
  price DOUBLE,
  raw JSON
);
