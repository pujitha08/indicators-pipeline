# src/fred_adapter.py
from __future__ import annotations
import datetime
from typing import List, Dict
from src.api_client import APIClient

# FRED series codes (you can add more if you like)
FRED_SERIES = {
    "CPIAUCSL": "Consumer Price Index (CPI)",
    "UNRATE": "Unemployment Rate",
    "SP500": "S&P 500 Index",
}

def fetch_series_data(client: APIClient, series_id: str, from_date: datetime.date, to_date: datetime.date) -> List[Dict]:
    """Fetch daily or monthly series data for the given series_id from FRED API."""
    params = {
        "series_id": series_id,
        "api_key": client.api_key,
        "file_type": "json",
        "observation_start": from_date.isoformat(),
        "observation_end": to_date.isoformat(),
    }

    payload = client.get_sync("/series/observations", params=params, use_cache=True, cache_ttl_s=24 * 3600)
    observations = payload.get("observations", [])
    rows = []
    for obs in observations:
        try:
            date_val = datetime.date.fromisoformat(obs["date"])
            value = float(obs["value"]) if obs["value"] not in ("", ".", None) else None
            rows.append({"as_of_date": date_val, "code": series_id, "value": value, "source": "FRED"})
        except Exception:
            continue
    return rows
