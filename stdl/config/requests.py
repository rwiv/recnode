from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    AFREECA_LIVE = "afreeca_live"
    TWITCH_LIVE = "twitch_live"
    YTDL_VIDEO = "ytdl_video"


@dataclass
class ChzzkVideoRequest:
    videoNoList: list[int]


@dataclass
class ChzzkLiveRequest:
    uid: str
    cookies: Optional[str]


@dataclass
class AfreecaLiveRequest:
    userId: str


@dataclass
class TwitchLiveRequest:
    channelName: str
    cookies: Optional[str]


@dataclass
class YtdlVideoRequest:
    urls: list[str]
