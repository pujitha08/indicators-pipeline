# src/ingestors.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
import duckdb
from typing import Iterable, List, Tuple
from src import config

# --- simple row models ---
@dataclass(frozen=True)
class IndicatorRow:
    as_of_date: date
    indicator_code: str
    value: float
    source: str = "demo"

@dataclass(frozen=True)
class IndexRow:
    as_of_date: date
    index_code: str
    value: float
    source: str = "demo"

# --- temporary generators (deterministic) ---
def generate_indicators(from_date: date, to_date: date) -> List[IndicatorRow]:
    """Generate 2 demo indicators per day for [from_date, to_date]."""
    rows: List[IndicatorRow] = []
    d = from_date
    while d <= to_date:
        # simple wave patterns (deterministic, no randomness)
        day_idx = (d - from_date).days
        rows.append(IndicatorRow(d, "CPI_DEMO", 100 + 0.1 * day_idx))
        rows.append(IndicatorRow(d, "UNRATE_DEMO", 4.5 + (day_idx % 7) * 0.05))
        d += timedelta(days=1)
    return rows

def generate_indices(from_date: date, to_date: date) -> List[IndexRow]:
    """Generate 1 demo index per day for [from_date, to_date]."""
    rows: List[IndexRow] = []
    d = from_date
    base = 4200.0
    while d <= to_date:
        day_idx = (d - from_date).days
        rows.append(IndexRow(d, "SPX_DEMO", base + 2.0 * day_idx))
        d += timedelta(days=1)
    return rows

# --- persistence helpers ---
def insert_indicators(rows: Iterable[IndicatorRow]) -> int:
    con = duckdb.connect(config.DB_PATH)
    data = [(r.as_of_date, r.indicator_code, r.value, r.source) for r in rows]
    # INSERT many rows at once
    con.executemany(
        "INSERT INTO indicators_raw(as_of_date, indicator_code, value, source) VALUES (?, ?, ?, ?)",
        data,
    )
    n = con.execute("SELECT COUNT(*) FROM indicators_raw").fetchone()[0]
    con.close()
    return n

def insert_indices(rows: Iterable[IndexRow]) -> int:
    con = duckdb.connect(config.DB_PATH)
    data = [(r.as_of_date, r.index_code, r.value, r.source) for r in rows]
    con.executemany(
        "INSERT INTO indices_raw(as_of_date, index_code, value, source) VALUES (?, ?, ?, ?)",
        data,
    )
    n = con.execute("SELECT COUNT(*) FROM indices_raw").fetchone()[0]
    con.close()
    return n
