from pydantic import BaseModel, constr

from .recording_schema import StreamInfo


class RecordingArgs(BaseModel):
    out_dir_path: constr(min_length=1)
    use_credentials: bool


class StreamlinkArgs(BaseModel):
    info: StreamInfo
    cookies: constr(min_length=1) | None = None
    options: dict[str, str] | None = None
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
