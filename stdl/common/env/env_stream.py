import os

from pydantic import BaseModel, conint, confloat


class StreamConfig(BaseModel):
    seg_size_mb: conint(ge=1) | None
    stream_timeout_sec: confloat(ge=1) | None


def read_stream_config() -> StreamConfig:
    return StreamConfig(
        seg_size_mb=os.getenv("SEG_SIZE_MB") or None,
        stream_timeout_sec=os.getenv("STREAM_TIMEOUT_SEC") or None,
    )
