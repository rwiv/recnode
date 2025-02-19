import sys

from .run_server import run_server
from .run_batch import BatchRunner

targets = ["main_router", "run_server", "run_batch"]
for name in list(sys.modules.keys()):
    for target in targets:
        if name.startswith(f"{__name__}.{target}"):
            sys.modules[name] = None  # type: ignore
