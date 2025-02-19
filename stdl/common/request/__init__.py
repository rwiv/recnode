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
from .request_config import AppConfig, read_config, read_app_config_by_file

targets = ["request_config", "request_config_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
