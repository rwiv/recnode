import os

from pydantic import BaseModel, conint


DEFAULT_SESSION_TIMEOUT_SEC = 30


class StreamConfig(BaseModel):
    seg_size_mb: conint(ge=1) | None = None
    read_session_timeout_sec: float = DEFAULT_SESSION_TIMEOUT_SEC


def read_stream_config() -> StreamConfig:
    read_session_timeout_sec = os.getenv("READ_SESSION_TIMEOUT_SEC")
    if read_session_timeout_sec is None:
        read_session_timeout_sec = DEFAULT_SESSION_TIMEOUT_SEC
    return StreamConfig(
        seg_size_mb=os.getenv("SEG_SIZE_MB"),
        read_session_timeout_sec=float(read_session_timeout_sec),
    )
