import os

from pydantic import BaseModel, conint


class WatcherConfig(BaseModel):
    parallel: conint(ge=1)
    threshold_sec: float


def read_watcher_config() -> WatcherConfig:
    return WatcherConfig(
        parallel=os.getenv("WATCHER_PARALLEL"),  # type: ignore
        threshold_sec=os.getenv("WATCHER_THRESHOLD_SEC"),  # type: ignore
    )
