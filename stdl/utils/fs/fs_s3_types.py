from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from stdl.utils.fs.fs_common_types import FileInfo


class S3ObjectInfoResponse(BaseModel):
    key: str
    meta: Any = Field(alias="ResponseMetadata")
    accept_ranges: str = Field(alias="AcceptRanges")
    last_modified: datetime = Field(alias="LastModified")
    content_length: int = Field(alias="ContentLength")
    etag: str = Field(alias="ETag")
    content_type: str = Field(alias="ContentType")

    @staticmethod
    def new(d: Any, key: str):
        return S3ObjectInfoResponse(**d, key=key)

    def to_file_info(self) -> FileInfo:
        path = self.key.rstrip("/")
        return FileInfo(
            name=path.rstrip("/"),
            path=path,
            is_dir=self.key[-1] == "/",
            size=self.content_length,
            mtime=self.last_modified,
        )


class S3ListContentObject(BaseModel):
    key: str = Field(alias="Key")
    last_modified: datetime = Field(alias="LastModified")
    etag: str = Field(alias="ETag")
    size: int = Field(alias="Size")
    storage_class: str = Field(alias="StorageClass")

    def to_file_info(self) -> FileInfo:
        path = self.key.rstrip("/")
        return FileInfo(
            name=path.rstrip("/"),
            path=path,
            is_dir=self.key[-1] == "/",
            size=self.size,
            mtime=self.last_modified,
        )


class S3ListPrefixObject(BaseModel):
    prefix: str = Field(alias="Prefix")


class S3ListResponse(BaseModel):
    meta: Any = Field(alias="ResponseMetadata")
    contents: list[S3ListContentObject] | None = Field(alias="Contents")
    prefixes: list[S3ListPrefixObject] | None = Field(alias="CommonPrefixes")

    @staticmethod
    def new(d: Any):
        if "Contents" not in d:
            d["Contents"] = None
        if "CommonPrefixes" not in d:
            d["CommonPrefixes"] = None
        return S3ListResponse(**d)
