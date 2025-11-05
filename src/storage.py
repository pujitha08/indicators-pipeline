import duckdb
import pathlib
from src import config

SCHEMA_PATH = pathlib.Path("sql/schema_duckdb.sql")
DB_PATH = pathlib.Path(config.DB_PATH)

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA_PATH.read_text())
    con.close()
    print(f"✅ DuckDB initialized at {DB_PATH.resolve()}")

if __name__ == "__main__":
    init_db()
