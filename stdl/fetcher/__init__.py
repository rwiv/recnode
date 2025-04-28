import os
import sys

from .fetcher import LiveInfo
from .live_url_resolver import resolve_live_url
from .platform.chzzk_fetcher import ChzzkFetcher
from .platform.soop_fetcher import SoopFetcher
from .platform.twitch_fetcher import TwitchFetcher
from .platform_fetcher import PlatformFetcher

targets = [
    "fetcher",
    "live_url_resolver",
    "platform",
    "platform_fetcher",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
