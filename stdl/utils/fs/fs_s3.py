from io import IOBase
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from stdl.common.fs_config import S3Config
from stdl.utils.fs.fs_common_abstract import AbstractFsAccessor
from stdl.utils.fs.fs_common_types import FileInfo
from stdl.utils.fs.fs_s3_types import S3ObjectInfoResponse, S3ListResponse
from stdl.utils.fs.fs_s3_utils import to_dir_path


class S3FsAccessor(AbstractFsAccessor):
    def __init__(self, config: S3Config):
        self.config = config
        self.bucket = config.bucket
        self.__s3 = self.__get_client()

    def head(self, path: str) -> FileInfo | None:
        try:
            s3_res = self.__s3.head_object(Bucket=self.bucket, Key=path)
            return S3ObjectInfoResponse.new(s3_res, key=path).to_file_info()
        except ClientError as e:
            res: Any = e.response
            if res["Error"]["Code"] == "404":
                return None
            else:
                raise e

    def get_list(self, dir_path: str) -> list[FileInfo]:
        keys = self.__get_keys(dir_path)
        result = []
        for k in keys:
            info = self.head(k)
            if info is not None:
                result.append(info)
        return result

    def __get_keys(self, dir_path: str) -> list[str]:
        s3_res = self.__s3.list_objects_v2(Bucket=self.bucket, Prefix=to_dir_path(dir_path), Delimiter="/")
        res = S3ListResponse.new(s3_res)
        result = []
        if res.prefixes:
            for o in res.prefixes:
                if o.prefix == to_dir_path(dir_path):
                    continue
                result.append(o.prefix)
        if res.contents:
            for o in res.contents:
                if o.key == to_dir_path(dir_path):
                    continue
                result.append(o.key)
        return result

    def all(self):
        s3_res = self.__s3.list_objects_v2(Bucket=self.bucket, Prefix="")
        res = S3ListResponse.new(s3_res)
        result = []
        if res.contents:
            for o in res.contents:
                result.append(o.to_file_info())
        return result

    def mkdir(self, dir_path: str):
        self.__s3.put_object(Bucket=self.bucket, Key=to_dir_path(dir_path))

    def read(self, path: str) -> IOBase:
        res = self.__s3.get_object(Bucket=self.bucket, Key=path)
        return res["Body"]

    def write(self, path: str, data: bytes | IOBase):
        if isinstance(data, bytes):
            self.__s3.put_object(Bucket=self.bucket, Key=path, Body=data)
        elif isinstance(data, IOBase):
            self.__s3.upload_fileobj(data, self.bucket, path)  # type: ignore

    def delete(self, path: str):
        self.__s3.delete_object(Bucket=self.bucket, Key=path)

    def __get_client(self) -> S3Client:
        return boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            verify=self.config.verify,
        )
