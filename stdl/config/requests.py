from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    AFREECA_LIVE = "afreeca_live"
    TWITCH_LIVE = "twitch_live"
    YTDL_VIDEO = "ytdl_video"
    HLS_M3U8 = "hls_m3u8"


@dataclass
class ChzzkVideoRequest:
    videoNoList: list[int]
    parallel: bool
    cookies: Optional[str]


@dataclass
class ChzzkLiveRequest:
    uid: str
    once: bool
    cookies: Optional[str]


@dataclass
class AfreecaLiveRequest:
    userId: str
    once: bool


@dataclass
class TwitchLiveRequest:
    channelName: str
    once: bool
    cookies: Optional[str]


@dataclass
class YtdlVideoRequest:
    urls: list[str]


@dataclass
class HlsM3u8Request:
    urls: list[str]
    cookies: Optional[str]
