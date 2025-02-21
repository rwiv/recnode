import os
from abc import ABC, abstractmethod
from io import BufferedReader
from pathlib import Path

from pyutils import filename, dirname

from .fs_config import S3Config
from .fs_types import FsType
from ...utils import create_client


class FsWriter(ABC):
    def __init__(self, fs_type: FsType):
        self.fs_type = fs_type

    @abstractmethod
    def normalize_base_path(self, base_path: str) -> str:
        pass

    @abstractmethod
    def write(self, path: str, data: bytes | BufferedReader) -> None:
        pass


class LocalFsWriter(FsWriter):
    def __init__(self, chunk_size: int = 4096):
        super().__init__(FsType.LOCAL)
        self.chunk_size = chunk_size

    def normalize_base_path(self, base_path: str) -> str:
        return base_path

    def write(self, path: str, data: bytes | BufferedReader) -> None:
        if not Path(dirname(path)).exists():
            os.makedirs(dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            if isinstance(data, bytes):
                f.write(data)
            elif isinstance(data, BufferedReader):
                while True:
                    chunk = data.read(self.chunk_size)
                    if not data:
                        break
                    f.write(chunk)
            else:
                raise ValueError("data must be bytes or BufferedReader")


class S3FsWriter(FsWriter):
    def __init__(self, conf: S3Config):
        super().__init__(FsType.S3)
        self.conf = conf
        self.bucket_name = conf.bucket_name
        self.__s3 = create_client(self.conf)

    def normalize_base_path(self, base_path: str) -> str:
        return filename(base_path, "/")

    def write(self, path: str, data: bytes | BufferedReader):
        if isinstance(data, bytes):
            self.__s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)
        elif isinstance(data, BufferedReader):
            self.__s3.upload_fileobj(data, self.bucket_name, path)
        else:
            raise ValueError("data must be bytes or BufferedReader")
