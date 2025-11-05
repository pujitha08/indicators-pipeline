import httpx
from src import config

class APIClient:
    def __init__(self):
        self.base_url = config.API_BASE_URL
        self.api_key = config.API_KEY

    async def get(self, endpoint: str, params: dict = None):
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        async with httpx.AsyncClient(base_url=self.base_url, headers=headers) as client:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
