import os
import sys

from .manager.recording_scheduler import RecordingScheduler
from .manager.recorder_resolver import RecorderResolver

targets = [
    "manger",
    "schema",
    "stream",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
