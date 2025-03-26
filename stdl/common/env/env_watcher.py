import os

from pydantic import BaseModel, conint


class WatcherConfig(BaseModel):
    enabled: bool
    parallel: conint(ge=1) | None = None
    threshold_sec: float | None = None
    interval_delay_sec: float | None = None


def read_watcher_config() -> WatcherConfig:
    return WatcherConfig(
        enabled=os.getenv("WATCHER_ENABLED") == "true",
        parallel=os.getenv("WATCHER_PARALLEL"),  # type: ignore
        threshold_sec=os.getenv("WATCHER_THRESHOLD_SEC"),  # type: ignore
        interval_delay_sec=os.getenv("WATCHER_INTERVAL_DELAY_SEC"),  # type: ignore
    )
