from pydantic import BaseModel, constr


class StreamLinkSessionArgs(BaseModel):
    cookie_header: constr(min_length=1) | None = None
    options: dict[str, str] | None = None
    # Read session timeout occurs when the internet connection is unstable
    stream_timeout_sec: float | None = None


class RecordingArgs(BaseModel):
    live_url: str
    session_args: StreamLinkSessionArgs
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
