import re

from streamlink.session.session import Streamlink

from .chzzk_fetcher import ChzzkFetcher
from .fetcher import LiveInfo
from .soop_fetcher import SoopFetcher
from .twitch_fetcher import TwitchFetcher


class PlatformFetcher:
    def __init__(self):
        self.__chzzk = ChzzkFetcher()
        self.__soop = SoopFetcher()
        self.__twitch = TwitchFetcher()

    async def fetch_live_info(self, live_url: str) -> LiveInfo | None:
        if "chzzk" in live_url:
            session = Streamlink()
            regex = re.compile(r"https?://chzzk\.naver\.com/live/(?P<channel_id>[^/?]+)")
            match = regex.match(live_url)
            if not match:
                raise ValueError("Invalid Chzzk URL")
            channel_id = match.group("channel_id")
            return await self.__chzzk.fetch_live_info(channel_id, session.http.headers)
        elif "soop" in live_url or "afreeca" in live_url:
            session = Streamlink()
            regex = re.compile(
                r"https?://play\.(sooplive\.co\.kr|afreecatv\.com)/(?P<channel>\w+)(?:/(?P<bno>\d+))?"
            )
            match = regex.match(live_url)
            if not match:
                raise ValueError("Invalid SOOP or Afreeca URL")
            channel_id = match.group("channel")
            return await self.__soop.fetch_live_info(channel_id, session.http.headers)
        elif "twitch" in live_url:
            regex = re.compile(
                r"https?://(?:(?!clips\.)[\w-]+\.)?twitch\.tv/(?P<channel>(?!v(?:ideos?)?/|clip/)[^/?]+)/?(?:\?|$)"
            )
            match = regex.match(live_url)
            if not match:
                raise ValueError("Invalid Twitch URL")
            channel_login = match.group("channel")
            return await self.__twitch.metadata_channel(channel_login)
        else:
            raise ValueError("Unsupported platform")
