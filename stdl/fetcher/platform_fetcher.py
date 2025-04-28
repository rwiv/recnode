from .fetcher import LiveInfo
from .live_url_resolver import resolve_live_url
from .platform.chzzk_fetcher import ChzzkFetcher
from .platform.soop_fetcher import SoopFetcher
from .platform.twitch_fetcher import TwitchFetcher
from ..common.spec import PlatformType


class PlatformFetcher:
    def __init__(self):
        self.__chzzk = ChzzkFetcher()
        self.__soop = SoopFetcher()
        self.__twitch = TwitchFetcher()
        self.headers = {}

    def set_headers(self, headers: dict):
        for k, v in headers.items():
            if self.headers.get(k) is not None:
                raise ValueError(f"Header {k} already set")
            self.headers[k] = v

    async def fetch_live_info(self, live_url: str) -> LiveInfo | None:
        url_info = resolve_live_url(live_url=live_url)
        if url_info.platform == PlatformType.CHZZK:
            return await self.__chzzk.fetch_live_info(url_info.channel_id, self.headers)
        elif url_info.platform == PlatformType.SOOP:
            return await self.__soop.fetch_live_info(url_info.channel_id, self.headers)
        elif url_info.platform == PlatformType.TWITCH:
            return await self.__twitch.metadata_channel(url_info.channel_id, self.headers)
        else:
            raise ValueError("Unsupported platform")
