from pydantic import BaseModel, constr

from .recording_schema import StreamInfo


class RecordingArgs(BaseModel):
    out_dir_path: constr(min_length=1)
    use_credentials: bool


class StreamLinkSessionArgs(BaseModel):
    cookies: constr(min_length=1) | None = None
    options: dict[str, str] | None = None
    # Read session timeout occurs when the internet connection is unstable
    read_session_timeout_sec: float


class StreamArgs(BaseModel):
    info: StreamInfo
    session_args: StreamLinkSessionArgs
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
