import asyncio
from abc import ABC, abstractmethod

import aiofiles
import aiohttp
from aiofiles import os as aos
from aiohttp import FormData
from pyutils import dirpath, log, error_dict, filename

from .fs_config import S3Config
from .fs_types import FsType
from .s3_utils import create_async_client
from ..common import LOCAL_FS_NAME
from ..metric import metric
from ..utils import HttpRequestError


class ObjectWriter(ABC):
    def __init__(self, fs_type: FsType, fs_name: str):
        self.fs_type = fs_type
        self.fs_name = fs_name
        self.write_retry_limit = 8
        self.write_retry_delay_sec = 0.5

    async def write(self, path: str, data: bytes) -> None:
        start = asyncio.get_event_loop().time()
        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                await self._write(path, data)
                break
            except Exception as e:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", error_dict(e))
                    raise
                log.error(f"Write Segment: cnt={retry_cnt}", error_dict(e))
                await asyncio.sleep(self.write_retry_delay_sec * (2**retry_cnt))
        metric.set_object_write_duration(asyncio.get_event_loop().time() - start)

    @abstractmethod
    async def _write(self, path: str, data: bytes) -> None:
        pass


class LocalObjectWriter(ObjectWriter):
    def __init__(self):
        super().__init__(FsType.LOCAL, LOCAL_FS_NAME)

    async def _write(self, path: str, data: bytes) -> None:
        if not await aos.path.exists(dirpath(path)):
            await aos.makedirs(dirpath(path), exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)


class S3ObjectWriter(ObjectWriter):
    def __init__(self, fs_name: str, conf: S3Config):
        super().__init__(FsType.S3, fs_name)
        self.conf = conf
        self.bucket_name = conf.bucket_name

    async def _write(self, path: str, data: bytes):
        async with create_async_client(self.conf) as client:
            res = await client.put_object(Bucket=self.bucket_name, Key=path, Body=data)
            status = res["ResponseMetadata"]["HTTPStatusCode"]
            if status >= 400:
                raise HttpRequestError("Failed to upload file", status=status)


class ProxyObjectWriter(ObjectWriter):
    def __init__(self, endpoint: str, fs_name: str):
        super().__init__(FsType.PROXY, fs_name)
        self.__endpoint = endpoint

    async def _write(self, path: str, data: bytes) -> None:
        url = f"{self.__endpoint}/api/upload"
        form = FormData()
        form.add_field("file", data, filename=filename(path), content_type="application/octet-stream")
        form.add_field("path", path)
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, data=form) as res:
                if res.status >= 400:
                    raise HttpRequestError.from_response("Failed to upload file", res=res)
