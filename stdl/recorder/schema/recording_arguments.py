from pydantic import BaseModel, constr

from .recording_schema import StreamInfo


class RecordingArgs(BaseModel):
    out_dir_path: constr(min_length=1) | None
    use_credentials: bool


class StreamLinkSessionArgs(BaseModel):
    cookie_header: constr(min_length=1) | None = None
    options: dict[str, str] | None = None
    # Read session timeout occurs when the internet connection is unstable
    stream_timeout_sec: float | None = None


class StreamArgs(BaseModel):
    info: StreamInfo
    session_args: StreamLinkSessionArgs
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
