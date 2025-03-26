import os
import sys

from .request_types import (
    RequestType,
    ChzzkVideoRequest,
    ChzzkLiveRequest,
    SoopLiveRequest,
    SoopVideoRequest,
    TwitchLiveRequest,
    YtdlVideoRequest,
    HlsM3u8Request,
)
from .request import AppRequest, read_request_by_env, read_request_by_file

targets = [
    "request",
    "request_types",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
