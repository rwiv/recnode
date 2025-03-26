import os
import sys

from .batch_runner import BatchRunner
from .server_runner import run_server
from .server_main_router import CancelRequest
from .watcher_runner import WatcherRunner

targets = [
    "batch_runner",
    "server_main_router",
    "server_runner",
    "watcher_runner",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
