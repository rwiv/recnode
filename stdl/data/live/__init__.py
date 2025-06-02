import os
import sys

from .live_state import LiveState
from .live_state_service import LiveStateService

targets = [
    "live_state",
    "live_state_service",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
