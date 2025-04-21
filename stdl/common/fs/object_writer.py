import os
from abc import ABC, abstractmethod
from pathlib import Path

import requests
from pyutils import filename, dirpath

from .fs_config import S3Config
from .fs_types import FsType
from ..spec import LOCAL_FS_NAME, PROXY_FS_NAME
from ...common.s3 import create_client
from ...utils import HttpRequestError


class ObjectWriter(ABC):
    def __init__(self, fs_type: FsType, fs_name: str):
        self.fs_type = fs_type
        self.fs_name = fs_name

    @abstractmethod
    def normalize_base_path(self, base_path: str) -> str:
        pass

    @abstractmethod
    def write(self, path: str, data: bytes) -> None:
        pass


class LocalObjectWriter(ObjectWriter):
    def __init__(self):
        super().__init__(FsType.LOCAL, LOCAL_FS_NAME)

    def normalize_base_path(self, base_path: str) -> str:
        return base_path

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

    def normalize_base_path(self, base_path: str) -> str:
        return filename(base_path)

    def write(self, path: str, data: bytes):
        self.__s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)


class ProxyObjectWriter(ObjectWriter):
    def __init__(self, endpoint: str):
        super().__init__(FsType.PROXY, PROXY_FS_NAME)
        self.__endpoint = endpoint

    def normalize_base_path(self, base_path: str) -> str:
        return base_path

    def write(self, path: str, data: bytes) -> None:
        url = f"{self.__endpoint}/api/upload"
        with open(path, "rb") as f:
            files = {"file": (filename(path), f)}
            res = requests.post(url, files=files)
            if res.status_code >= 400:
                raise HttpRequestError.from_response("Failed to upload file", res=res)
