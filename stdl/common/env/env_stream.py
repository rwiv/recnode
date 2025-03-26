import os

from pydantic import BaseModel, conint


DEFAULT_STREAM_TIMEOUT_SEC = 30


class StreamConfig(BaseModel):
    seg_size_mb: conint(ge=1) | None = None
    stream_timeout_sec: float


def read_stream_config() -> StreamConfig:
    stream_timeout_sec = os.getenv("STREAM_TIMEOUT_SEC")
    if stream_timeout_sec is None:
        stream_timeout_sec = DEFAULT_STREAM_TIMEOUT_SEC
    return StreamConfig(
        seg_size_mb=os.getenv("SEG_SIZE_MB"),
        stream_timeout_sec=stream_timeout_sec,  # type: ignore
    )
