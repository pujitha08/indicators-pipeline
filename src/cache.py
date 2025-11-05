# src/cache.py
import hashlib, json, pathlib, time
from typing import Optional

CACHE_DIR = pathlib.Path("cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _key(url: str, params: dict | None) -> str:
    j = json.dumps({"url": url, "params": params or {}}, sort_keys=True).encode()
    return hashlib.sha256(j).hexdigest()

def read_cache(url: str, params: dict | None = None, ttl_seconds: int = 24 * 3600) -> Optional[dict]:
    k = _key(url, params)
    p = CACHE_DIR / f"{k}.json"
    if not p.exists():
        return None
    if ttl_seconds > 0 and (time.time() - p.stat().st_mtime) > ttl_seconds:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None

def write_cache(url: str, params: dict | None, payload: dict) -> None:
    k = _key(url, params)
    p = CACHE_DIR / f"{k}.json"
    p.write_text(json.dumps(payload))
