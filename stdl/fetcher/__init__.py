import os
import sys

from .fetcher import LiveInfo
from .chzzk_fetcher import ChzzkFetcher
from .soop_fetcher import SoopFetcher
from .twitch_fetcher import TwitchFetcher
from .platform_fetcher import PlatformFetcher

targets = [
    "chzzk_fetcher",
    "fetcher",
    "platform_fetcher",
    "soop_fetcher",
    "twitch_fetcher",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
