import asyncio
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path

import aiofiles
import aiohttp
from aiohttp import FormData
from pyutils import dirpath

from .fs_config import S3Config
from .fs_types import FsType
from ..s3.s3_async_utils import create_async_client
from ...common import LOCAL_FS_NAME
from ...metric import MetricManager
from ...utils import HttpRequestError


class AsyncObjectWriter(ABC):
    def __init__(self, fs_type: FsType, fs_name: str, metric: MetricManager):
        self.fs_type = fs_type
        self.fs_name = fs_name
        self.metric = metric

    async def write(self, path: str, data: bytes) -> None:
        start = time.time()
        await self._write(path, data)
        self.metric.set_object_write_duration(time.time() - start)

    @abstractmethod
    async def _write(self, path: str, data: bytes) -> None:
        pass


class LocalAsyncObjectWriter(AsyncObjectWriter):
    def __init__(self, metric: MetricManager):
        super().__init__(FsType.LOCAL, LOCAL_FS_NAME, metric)

    async def _write(self, path: str, data: bytes) -> None:
        await asyncio.to_thread(check_dir, path)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)


class S3AsyncObjectWriter(AsyncObjectWriter):
    def __init__(self, fs_name: str, conf: S3Config, metric: MetricManager):
        super().__init__(FsType.S3, fs_name, metric)
        self.conf = conf
        self.bucket_name = conf.bucket_name

    async def _write(self, path: str, data: bytes):
        async with create_async_client(self.conf) as client:
            res = await client.put_object(Bucket=self.bucket_name, Key=path, Body=data)
            status = res["ResponseMetadata"]["HTTPStatusCode"]
            if status >= 400:
                raise HttpRequestError("Failed to upload file", status=status)


class ProxyAsyncObjectWriter(AsyncObjectWriter):
    def __init__(self, endpoint: str, fs_name: str, metric: MetricManager):
        super().__init__(FsType.PROXY, fs_name, metric)
        self.__endpoint = endpoint

    async def _write(self, path: str, data: bytes) -> None:
        url = f"{self.__endpoint}/api/upload"
        form = FormData()
        form.add_field("file", data, filename=path, content_type="application/octet-stream")
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, data=form) as res:
                if res.status >= 400:
                    raise HttpRequestError.from_response2("Failed to upload file", res=res)


def check_dir(path: str):
    if not Path(dirpath(path)).exists():
        os.makedirs(dirpath(path), exist_ok=True)
