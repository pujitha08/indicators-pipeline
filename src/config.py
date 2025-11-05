import os

DB_PATH = os.getenv("DB_PATH", "./data/warehouse.duckdb")
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.example.com")
API_KEY = os.getenv("API_KEY", "")
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "20"))
MONTHLY_CAP = int(os.getenv("MONTHLY_CAP", "500"))
