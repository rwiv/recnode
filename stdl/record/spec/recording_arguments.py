from pydantic import BaseModel, Field

from stdl.common.types import PlatformType


class RecorderArgs(BaseModel):
    out_dir_path: str = Field(min_length=1)
    platform_type: PlatformType
    use_credentials: bool


class StreamlinkArgs(BaseModel):
    url: str = Field(min_length=1)
    uid: str = Field(min_length=1)
    cookies: str | None = Field(min_length=1, default=None)
    options: dict[str, str] | None = Field(min_length=1, default=None)
