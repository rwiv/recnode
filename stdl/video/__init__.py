import os
import sys

from .chzzk.chzzk_video_downloader import ChzzkVideoDownloader
from .chzzk.chzzk_video_downloader_legacy import ChzzkVideoDownloaderLegacy
from .soop.soop_video_downloader import SoopVideoDownloader

targets = [
    "chzzk",
    "soop",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
