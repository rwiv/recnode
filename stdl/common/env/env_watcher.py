import os

from pydantic import BaseModel, conint, confloat


class WatcherConfig(BaseModel):
    enabled: bool
    parallel: conint(ge=1) | None
    threshold_sec: confloat(ge=0) | None
    interval_delay_sec: confloat(ge=0) | None


def read_watcher_config() -> WatcherConfig:
    return WatcherConfig(
        enabled=os.getenv("WATCHER_ENABLED") == "true",
        parallel=os.getenv("WATCHER_PARALLEL") or None,
        threshold_sec=os.getenv("WATCHER_THRESHOLD_SEC") or None,
        interval_delay_sec=os.getenv("WATCHER_INTERVAL_DELAY_SEC") or None,
    )
