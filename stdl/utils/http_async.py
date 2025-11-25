import asyncio
from enum import Enum
from typing import Any

import aiohttp
from aiohttp import BaseConnector, ClientTimeout
from aiohttp_socks import ProxyConnector, ProxyType
import rust_downloader
from pydantic import BaseModel
from pyutils import log, error_dict

from .errors import HttpRequestError


class ReturnType(Enum):
    TEXT = "text"
    JSON = "json"
    RAW = "raw"


class ProxyConnectorConfig(BaseModel):
    proxy_type: ProxyType
    host: str
    port: int
    username: str
    password: str
    rdns: bool


class AsyncHttpClient:
    def __init__(
        self,
        timeout_sec: float = 60,
        retry_limit: int = 0,
        retry_delay_sec: float = 0,
        use_backoff: bool = False,
        print_error: bool = True,
        proxy: ProxyConnectorConfig | None = None,
    ):
        self.retry_limit = retry_limit
        self.retry_delay_sec = retry_delay_sec
        self.use_backoff = use_backoff
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self.headers = {}
        self.print_error = print_error
        self.proxy_connector_config = proxy

    def set_headers(self, headers: dict):
        for k, v in headers.items():
            if self.headers.get(k) is not None:
                raise ValueError(f"Header {k} already set")
            self.headers[k] = v

    def __proxy_connector(self) -> BaseConnector | None:
        if self.proxy_connector_config is None:
            return None

        return ProxyConnector(
            proxy_type=ProxyType.SOCKS5,
            host=self.proxy_connector_config.host,
            port=self.proxy_connector_config.port,
            username=self.proxy_connector_config.username,
            password=self.proxy_connector_config.password,
            rdns=self.proxy_connector_config.rdns,
        )

    async def get_text(
        self,
        url: str,
        headers: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> str:
        return await self.fetch(
            method="GET",
            url=url,
            headers=headers,
            return_type=ReturnType.TEXT,
            attr=attr,
            print_error=print_error,
            retry_limit=retry_limit,
            connector=connector,
        )

    async def get_json(
        self,
        url: str,
        headers: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> Any:
        return await self.fetch(
            method="GET",
            url=url,
            headers=headers,
            return_type=ReturnType.JSON,
            attr=attr,
            print_error=print_error,
            retry_limit=retry_limit,
            connector=connector,
        )

    async def get_bytes(
        self,
        url: str,
        headers: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> bytes:
        return await self.fetch(
            method="GET",
            url=url,
            headers=headers,
            return_type=ReturnType.RAW,
            attr=attr,
            print_error=print_error,
            retry_limit=retry_limit,
            connector=connector,
        )

    async def post_json(
        self,
        url: str,
        json: dict,
        headers: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> Any:
        return await self.fetch(
            method="POST",
            url=url,
            headers=headers,
            return_type=ReturnType.JSON,
            json=json,
            attr=attr,
            print_error=print_error,
            retry_limit=retry_limit,
            connector=connector,
        )

    async def request_file_text(self, url: str, attr: dict | None = None) -> str:
        start = asyncio.get_event_loop().time()
        try:
            status, _, content = await rust_downloader.request_file(url, self.headers, None, True)  # type: ignore
            if status >= 400:
                log.error("Failed to request", get_err_dict(url, start, attr, status=status))
                raise HttpRequestError("Failed to request", status)
            return content.decode("utf-8")
        except RuntimeError as ex:
            log.error("Failed to request", get_err_dict(url, start, attr, status=500))
            raise HttpRequestError("Failed to request", 500) from ex

    async def request_file(self, url: str, file_path: str | None, attr: dict | None = None) -> int:
        start = asyncio.get_event_loop().time()
        try:
            status, size, _ = await rust_downloader.request_file(url, self.headers, file_path, False)  # type: ignore
            if status >= 400:
                log.error("Failed to request", get_err_dict(url, start, attr, status=status))
                raise HttpRequestError("Failed to request", status)
            return size
        except RuntimeError as ex:
            log.error("Failed to request", get_err_dict(url, start, attr, status=500))
            raise HttpRequestError("Failed to request", 500) from ex

    async def fetch(
        self,
        method: str,
        url: str,
        return_type: ReturnType,
        headers: dict | None = None,
        json: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> Any:
        req_headers = self.headers
        if headers is not None:
            req_headers = self.headers.copy()
            for key, value in headers.items():
                req_headers[key] = value

        req_print_error = print_error if print_error is not None else self.print_error
        req_retry_limit = retry_limit if retry_limit is not None else self.retry_limit

        proxy_connector = connector
        if proxy_connector is None:
            proxy_connector = self.__proxy_connector()

        for retry_cnt in range(req_retry_limit + 1):
            start = asyncio.get_event_loop().time()
            try:
                return await request(
                    method=method,
                    url=url,
                    return_type=return_type,
                    headers=req_headers,
                    json=json,
                    timeout=self.timeout,
                    connector=proxy_connector,
                )
            except Exception as ex:
                err = error_dict(ex)
                err["url"] = url
                err["retry_cnt"] = retry_cnt
                err["duration"] = round(asyncio.get_event_loop().time() - start, 2)
                if isinstance(ex, HttpRequestError):
                    err["status"] = ex.status
                    err["method"] = ex.method
                    err["reason"] = ex.reason
                if attr is not None:
                    for k, v in attr.items():
                        err[k] = v

                if self.retry_limit == 0:
                    if req_print_error:
                        log.error("Failed to request", err)
                    raise
                if retry_cnt == self.retry_limit:
                    if req_print_error:
                        log.error("Failed to request: Retry Limit Exceeded", err)
                    raise

                if req_print_error:
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
    return_type: ReturnType,
    json: dict | None = None,
    timeout: ClientTimeout = ClientTimeout(total=60),
    connector: BaseConnector | None = None,
) -> Any:
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.request(method=method, url=url, headers=headers, json=json) as res:
            if res.status >= 400:
                raise HttpRequestError("Failed to request", res.status, url, res.method, res.reason)
            if return_type == ReturnType.TEXT:
                return await res.text()
            elif return_type == ReturnType.JSON:
                return await res.json()
            elif return_type == ReturnType.RAW:
                return await res.read()
            else:
                raise ValueError(f"Invalid return type: {return_type}")


class AsyncHttpClientMock(AsyncHttpClient):
    def __init__(self, b_size: int):
        super().__init__()
        self.b_size = b_size

    async def get_bytes(
        self,
        url: str,
        headers: dict | None = None,
        attr: dict | None = None,
        print_error: bool | None = None,
        retry_limit: int | None = None,
        connector: BaseConnector | None = None,
    ) -> bytes:
        return b"0" * self.b_size


def get_err_dict(url: str, start: float, attr: dict | None = None, status: int | None = None) -> dict:
    err = {"url": url, "duration": round(asyncio.get_event_loop().time() - start, 2)}
    if status is not None:
        err["status"] = status
    if attr is not None:
        for k, v in attr.items():
            err[k] = v
    return err
