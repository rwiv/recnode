import os
import sys

from .common_constants import LOCAL_FS_NAME, PROXY_FS_NAME
from .common_types import PlatformType

targets = [
    "common_constants",
    "common_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
