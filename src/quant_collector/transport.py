import asyncio
import json
import time
from typing import Any

import aiohttp


class PublicHTTP:
    """One limiter per exchange, shared by discovery and background polls."""

    def __init__(self, session: aiohttp.ClientSession, requests_per_second: float = 3):
        self.session = session
        self.interval = 1 / requests_per_second
        self.lock = asyncio.Lock()
        self.next_request = 0.0

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        for attempt in range(5):
            async with self.lock:
                await asyncio.sleep(max(0, self.next_request - time.monotonic()))
                self.next_request = time.monotonic() + self.interval
            async with self.session.request(method, url, **kwargs) as response:
                if response.status == 429 or response.status >= 500:
                    retry = response.headers.get("Retry-After", "")
                    delay = float(retry) if retry.isdigit() else min(30, 2 ** attempt)
                    await response.read()
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                data = json.loads(await response.text())
                if isinstance(data, dict):
                    if data.get("retCode", 0) != 0:
                        raise ValueError("Bybit API error code: %s" % data["retCode"])
                    if "code" in data and int(data["code"]) not in (0, 200):
                        raise ValueError("Public API error code: %s" % data["code"])
                return data
        raise RuntimeError("Public API retries exhausted: " + url.split("?")[0])

    async def get(self, url: str, **kwargs: Any) -> Any:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, body: Any) -> Any:
        return await self.request("POST", url, json=body)
