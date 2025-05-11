import asyncio
import time

from .fetcher import LiveInfo
from .live_url_resolver import resolve_live_url
from .platform.chzzk_fetcher import ChzzkFetcher
from .platform.soop_fetcher import SoopFetcher
from .platform.twitch_fetcher import TwitchFetcher
from ..common import PlatformType
from ..metric import MetricManager


class PlatformFetcher:
    def __init__(self, metric: MetricManager):
        self.__chzzk = ChzzkFetcher()
        self.__soop = SoopFetcher()
        self.__twitch = TwitchFetcher()
        self.__metric = metric
        self.headers = {}

    def set_headers(self, headers: dict):
        for k, v in headers.items():
            if self.headers.get(k) is not None:
                raise ValueError(f"Header {k} already set")
            self.headers[k] = v

    async def fetch_live_info(self, live_url: str) -> LiveInfo | None:
        url_info = resolve_live_url(live_url=live_url)
        start_time = asyncio.get_event_loop().time()
        if url_info.platform == PlatformType.CHZZK:
            result = await self.__chzzk.fetch_live_info(url_info.channel_id, self.headers)
        elif url_info.platform == PlatformType.SOOP:
            result = await self.__soop.fetch_live_info(url_info.channel_id, self.headers)
        elif url_info.platform == PlatformType.TWITCH:
            result = await self.__twitch.metadata_channel(url_info.channel_id, self.headers)
        else:
            raise ValueError("Unsupported platform")
        duration = asyncio.get_event_loop().time() - start_time
        await self.__metric.set_api_request_duration(duration=duration, platform=url_info.platform)
        return result
