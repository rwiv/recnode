import asyncio
import time
from typing import Any

import aiohttp
from aiohttp import ClientTimeout
from pyutils import log, error_dict

from .errors import HttpRequestError


class AsyncHttpClient:
    def __init__(
        self,
        timeout_sec: float = 60,
        retry_limit: int = 0,
        retry_delay_sec: float = 0,
        use_backoff: bool = False,
        print_error: bool = True,
    ):
        self.retry_limit = retry_limit
        self.retry_delay_sec = retry_delay_sec
        self.use_backoff = use_backoff
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self.headers = {}
        self.print_error = print_error

    def set_headers(self, headers: dict):
        for k, v in headers.items():
            if self.headers.get(k) is not None:
                raise ValueError(f"Header {k} already set")
            self.headers[k] = v

    async def get_text(
        self, url: str, headers: dict, attr: dict | None = None, print_error: bool | None = None
    ) -> str:
        if print_error is None:
            print_error = self.print_error
        return await self.fetch(
            method="GET", url=url, headers=headers, text=True, attr=attr, print_error=print_error
        )

    async def get_json(
        self, url: str, headers: dict, attr: dict | None = None, print_error: bool | None = None
    ) -> Any:
        if print_error is None:
            print_error = self.print_error
        return await self.fetch(
            method="GET", url=url, headers=headers, json=True, attr=attr, print_error=print_error
        )

    async def get_bytes(
        self, url: str, headers: dict, attr: dict | None = None, print_error: bool | None = None
    ) -> bytes:
        if print_error is None:
            print_error = self.print_error
        return await self.fetch(
            method="GET", url=url, headers=headers, raw=True, attr=attr, print_error=print_error
        )

    async def fetch(
        self,
        method: str,
        url: str,
        headers: dict,
        text: bool = False,
        json: bool = False,
        raw: bool = False,
        attr: dict | None = None,
        print_error: bool = True,
    ) -> Any:
        req_headers = self.headers.copy()
        for key, value in headers.items():
            req_headers[key] = value

        for retry_cnt in range(self.retry_limit + 1):
            start = time.time()
            try:
                return await request(
                    method=method,
                    url=url,
                    headers=req_headers,
                    text=text,
                    json=json,
                    raw=raw,
                    timeout=self.timeout,
                )
            except Exception as ex:
                err = error_dict(ex)
                err["url"] = url
                err["retry_cnt"] = retry_cnt
                err["elapsed_time"] = round(time.time() - start, 2)
                if isinstance(ex, HttpRequestError):
                    err["status"] = ex.status
                    err["method"] = ex.method
                    err["reason"] = ex.reason
                if attr is not None:
                    for k, v in attr.items():
                        err[k] = v

                if self.retry_limit == 0 or retry_cnt == self.retry_limit:
                    if print_error:
                        log.error("Failed to request: Retry Limit Exceeded", err)
                    raise

                if print_error:
                    log.debug(f"Retry request", err)

                if self.retry_delay_sec >= 0:
                    if self.use_backoff:
                        await asyncio.sleep(self.retry_delay_sec * (2**retry_cnt))
                    else:
                        await asyncio.sleep(self.retry_delay_sec)


async def request(
    method: str,
    url: str,
    headers: dict,
    text: bool = False,
    json: bool = False,
    raw: bool = False,
    timeout: ClientTimeout = ClientTimeout(total=60),
) -> Any:
    async with aiohttp.ClientSession(timeout=timeout) as session:
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
