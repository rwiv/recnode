import asyncio
from typing import Any

import aiohttp
from pyutils import log, error_dict

from .errors import HttpRequestError


class AsyncHttpClient:
    def __init__(self, retry_limit: int = 0, retry_delay_sec: float = 0, use_backoff: bool = False):
        self.retry_limit = retry_limit
        self.retry_delay_sec = retry_delay_sec
        self.use_backoff = use_backoff
        self.headers = {}

    def set_headers(self, headers: dict):
        for k, v in headers.items():
            self.headers[k] = v

    async def get_text(self, url: str, headers: dict) -> str:
        return await self.fetch(method="GET", url=url, headers=headers, text=True)

    async def get_json(self, url: str, headers: dict) -> Any:
        return await self.fetch(method="GET", url=url, headers=headers, json=True)

    async def get_bytes(self, url: str, headers: dict) -> bytes:
        return await self.fetch(method="GET", url=url, headers=headers, raw=True)

    async def fetch(
        self, method: str, url: str, headers: dict, text: bool = False, json: bool = False, raw: bool = False
    ) -> Any:
        req_headers = self.headers.copy()
        for key, value in headers.items():
            req_headers[key] = value

        for retry_cnt in range(self.retry_limit + 1):
            try:
                return await request(
                    method=method, url=url, headers=req_headers, text=text, json=json, raw=raw
                )
            except Exception as ex:
                err = error_dict(ex)
                err["url"] = url
                err["retry_cnt"] = retry_cnt
                if isinstance(ex, HttpRequestError):
                    err["status"] = ex.status
                    err["method"] = ex.method
                    err["reason"] = ex.reason

                if retry_cnt == 0:
                    log.error("Failed to request", err)
                    raise

                if retry_cnt == self.retry_limit:
                    log.error("Retry Limit Exceeded", err)
                    raise

                log.error(f"Retry request", err)

                if self.retry_delay_sec >= 0:
                    if self.use_backoff:
                        await asyncio.sleep(self.retry_delay_sec * (2**retry_cnt))
                    else:
                        await asyncio.sleep(self.retry_delay_sec)


async def request(
    method: str, url: str, headers: dict, text: bool = False, json: bool = False, raw: bool = False
) -> Any:
    async with aiohttp.ClientSession() as session:
        async with session.request(method=method, url=url, headers=headers) as res:
            if res.status >= 400:
                raise HttpRequestError("Failed to request", res.status, url, res.method, res.reason)
            if text:
                return await res.text()
            elif raw:
                return await res.read()
            elif json:
                return await res.json()
            else:
                return await res.text()
