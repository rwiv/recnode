import re

from pydantic import BaseModel

from ..common.spec import PlatformType


class LiveUrlInfo(BaseModel):
    platform: PlatformType
    channel_id: str


def resolve_live_url(live_url: str) -> LiveUrlInfo:
    if "chzzk" in live_url:
        regex = re.compile(r"https?://chzzk\.naver\.com/live/(?P<channel_id>[^/?]+)")
        match = regex.match(live_url)
        if not match:
            raise ValueError("Invalid Chzzk URL")
        channel_id = match.group("channel_id")
        return LiveUrlInfo(platform=PlatformType.CHZZK, channel_id=channel_id)
    elif "soop" in live_url or "afreeca" in live_url:
        regex = re.compile(
            r"https?://play\.(sooplive\.co\.kr|afreecatv\.com)/(?P<channel>\w+)(?:/(?P<bno>\d+))?"
        )
        match = regex.match(live_url)
        if not match:
            raise ValueError("Invalid SOOP or Afreeca URL")
        channel_id = match.group("channel")
        return LiveUrlInfo(platform=PlatformType.SOOP, channel_id=channel_id)
    elif "twitch" in live_url:
        regex = re.compile(
            r"https?://(?:(?!clips\.)[\w-]+\.)?twitch\.tv/(?P<channel>(?!v(?:ideos?)?/|clip/)[^/?]+)/?(?:\?|$)"
        )
        match = regex.match(live_url)
        if not match:
            raise ValueError("Invalid Twitch URL")
        channel_login = match.group("channel")
        return LiveUrlInfo(platform=PlatformType.TWITCH, channel_id=channel_login)
    else:
        raise ValueError("Unsupported platform")
