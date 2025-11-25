import os

from pydantic import BaseModel, conint, confloat


class RequestConfig(BaseModel):
    m3u8_retry_limit: conint(ge=0)
    m3u8_timeout_sec: confloat(ge=1)
    seg_timeout_sec: confloat(ge=1)
    seg_parallel_retry_limit: conint(ge=0)
    seg_failure_threshold_ratio: conint(ge=0)
    interval_wait_weight_sec: confloat(ge=0)
    interval_min_time_sec: confloat(ge=0)


def read_request_config() -> RequestConfig:
    return RequestConfig(
        m3u8_retry_limit=os.getenv("M3U8_RETRY_LIMIT"),  # type: ignore
        m3u8_timeout_sec=os.getenv("M3U8_TIMEOUT_SEC"),  # type: ignore
        seg_timeout_sec=os.getenv("SEG_TIMEOUT_SEC"),  # type: ignore
        seg_parallel_retry_limit=os.getenv("SEG_PARALLEL_RETRY_LIMIT"),  # type: ignore
        seg_failure_threshold_ratio=os.getenv("SEG_FAILURE_THRESHOLD_RATIO"),  # type: ignore
        interval_wait_weight_sec=os.getenv("INTERVAL_WAIT_WEIGHT_SEC"),  # type: ignore
        interval_min_time_sec=os.getenv("INTERVAL_MIN_TIME_SEC"),  # type: ignore
    )
