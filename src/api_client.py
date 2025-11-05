# src/api_client.py
import httpx
import asyncio
from src import config
from src.cache import read_cache, write_cache
from src.quota import check_monthly_cap, record_call
from src.ratelimiter import acquire


class APIClient:
    def __init__(self):
        self.base_url = config.API_BASE_URL
        self.api_key = config.API_KEY

    async def get(self, endpoint: str, params: dict = None, use_cache: bool = True, cache_ttl_s: int = 24 * 3600):
        """
        GET request with:
        cache
        rate limiting
        monthly API cap enforcement
        """
        url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")

        # 1) cache-first
        if use_cache:
            cached = read_cache(url, params, ttl_seconds=cache_ttl_s)
            if cached is not None:
                return cached

        # 2) quota & rate limit
        check_monthly_cap()
        acquire()  # may sleep to respect RATE_LIMIT_PER_MIN

        # 3) make request
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        async with httpx.AsyncClient(base_url=self.base_url, headers=headers, timeout=30.0) as client:
            resp = await client.get(endpoint, params=params)
            resp.raise_for_status()
            data = resp.json()

        # 4) cache + record usage
        write_cache(url, params, data)
        record_call(1)
        return data

    # sync convenience wrapper
    def get_sync(self, endpoint: str, params: dict = None, **kw):
        return asyncio.run(self.get(endpoint, params=params, **kw))
