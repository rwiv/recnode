import os
from abc import ABC, abstractmethod
from pathlib import Path

import requests
from pyutils import dirpath

from .fs_config import S3Config
from .fs_types import FsType
from ..s3.s3_utils import create_client
from ...common import LOCAL_FS_NAME
from ...utils import HttpRequestError


class ObjectWriter(ABC):
    def __init__(self, fs_type: FsType, fs_name: str):
        self.fs_type = fs_type
        self.fs_name = fs_name

    @abstractmethod
    def write(self, path: str, data: bytes) -> None:
        pass


class LocalObjectWriter(ObjectWriter):
    def __init__(self):
        super().__init__(FsType.LOCAL, LOCAL_FS_NAME)

    def write(self, path: str, data: bytes) -> None:
        if not Path(dirpath(path)).exists():
            os.makedirs(dirpath(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)


class S3ObjectWriter(ObjectWriter):
    def __init__(self, fs_name: str, conf: S3Config):
        super().__init__(FsType.S3, fs_name)
        self.conf = conf
        self.bucket_name = conf.bucket_name
        self.__s3 = create_client(self.conf)

    def write(self, path: str, data: bytes):
        self.__s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)


class ProxyObjectWriter(ObjectWriter):
    def __init__(self, endpoint: str, fs_name: str):
        super().__init__(FsType.PROXY, fs_name)
        self.__endpoint = endpoint

    def write(self, path: str, data: bytes) -> None:
        url = f"{self.__endpoint}/api/upload"
        files = {"file": (path, data)}
        res = requests.post(url, files=files)
        if res.status_code >= 400:
            raise HttpRequestError.from_response("Failed to upload file", res=res)
