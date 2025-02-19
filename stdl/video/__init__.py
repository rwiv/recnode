import sys

from .chzzk.video_downloader import ChzzkVideoDownloader
from .chzzk.video_downloader_legacy import ChzzkVideoDownloaderLegacy
from .soop.video_downloader import SoopVideoDownloader

targets = ["chzzk", "soop"]
for name in list(sys.modules.keys()):
    for target in targets:
        if name.startswith(f"{__name__}.{target}"):
            sys.modules[name] = None  # type: ignore
