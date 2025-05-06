import os

from pydantic import BaseModel, conint, confloat


class RequestConfig(BaseModel):
    default_timeout_sec: confloat(ge=1)
    seg_timeout_sec: confloat(ge=1)
    seg_parallel_retry_limit: conint(ge=0)
    failed_count_threshold_ratio: conint(ge=0)


def read_request_config() -> RequestConfig:
    return RequestConfig(
        default_timeout_sec=os.getenv("DEFAULT_TIMEOUT_SEC"),  # type: ignore
        seg_timeout_sec=os.getenv("SEG_TIMEOUT_SEC"),  # type: ignore
        seg_parallel_retry_limit=os.getenv("SEG_PARALLEL_RETRY_LIMIT"),  # type: ignore
        failed_count_threshold_ratio=os.getenv("FAILED_COUNT_THRESHOLD_RATIO"),  # type: ignore
    )
