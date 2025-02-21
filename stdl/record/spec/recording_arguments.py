from pydantic import BaseModel, constr

from ...common.spec import PlatformType


class RecorderArgs(BaseModel):
    out_dir_path: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
    platform_type: PlatformType
    use_credentials: bool


class StreamlinkArgs(BaseModel):
    url: constr(min_length=1)
    uid: constr(min_length=1)
    cookies: constr(min_length=1) | None = None
    options: dict[str, str] | None = None
