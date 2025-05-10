from prometheus_client import Histogram as PromHistogram, Counter

from .buckets import (
    segment_request_duration_buckets,
    api_request_duration_buckets,
    m3u8_request_duration_buckets,
    segment_request_retry_buckets,
    object_write_duration_buckets,
)
from .histogram import Histogram
from ..common import PlatformType
from ..utils import AsyncCounter


class MetricManager:
    def __init__(self):
        self.api_request_duration_hist = PromHistogram(
            "api_request_duration_seconds",
            "Duration of HTTP API requests in seconds",
            ["platform"],
            buckets=api_request_duration_buckets,
        )
        self.m3u8_request_duration_hist = PromHistogram(
            "m3u8_request_duration_seconds",
            "Duration of HLS m3u8 requests in seconds",
            ["platform"],
            buckets=m3u8_request_duration_buckets,
        )
        self.segment_request_duration_hist = PromHistogram(
            "segment_request_duration_seconds",
            "Duration of HLS segment requests in seconds",
            ["platform"],
            buckets=segment_request_duration_buckets,
        )
        self.segment_request_retry_hist = PromHistogram(
            "segment_request_retry",
            "Retry counts of HLS segment requests in seconds",
            ["platform"],
            buckets=segment_request_retry_buckets,
        )
        self.segment_request_failures_counter = Counter(
            "segment_request_failures",
            "Count of HLS segment request failures",
            ["platform"],
        )
        self.object_write_duration_hist = PromHistogram(
            "object_write_duration_seconds",
            "Duration of object write requests in seconds",
            buckets=object_write_duration_buckets,
        )

    async def set_api_request_duration(self, duration: float, platform: PlatformType, extra: Histogram | None = None):
        self.api_request_duration_hist.labels(platform=platform.value).observe(duration)
        if extra is not None:
            await extra.observe(duration)

    async def set_m3u8_request_duration(self, duration: float, platform: PlatformType, extra: Histogram | None = None):
        self.m3u8_request_duration_hist.labels(platform=platform.value).observe(duration)
        if extra is not None:
            await extra.observe(duration)

    async def set_segment_request_duration(
        self, duration: float, platform: PlatformType, extra: Histogram | None = None
    ):
        self.segment_request_duration_hist.labels(platform=platform.value).observe(duration)
        if extra is not None:
            await extra.observe(duration)

    async def set_segment_request_retry(self, retry_cnt: int, platform: PlatformType, extra: Histogram | None = None):
        self.segment_request_retry_hist.labels(platform=platform.value).observe(retry_cnt)
        if extra is not None:
            await extra.observe(retry_cnt)

    async def inc_segment_request_failures(self, platform: PlatformType, extra: AsyncCounter | None = None):
        self.segment_request_failures_counter.labels(platform=platform.value).inc()
        if extra is not None:
            await extra.increment()

    def set_object_write_duration(self, duration: float):
        self.object_write_duration_hist.observe(duration)

    def create_m3u8_request_duration_histogram(self):
        return Histogram(m3u8_request_duration_buckets)

    def create_segment_request_duration_histogram(self):
        return Histogram(segment_request_duration_buckets)

    def create_segment_request_retry_histogram(self):
        return Histogram(segment_request_retry_buckets)
