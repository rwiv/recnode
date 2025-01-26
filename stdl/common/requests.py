from dataclasses import dataclass
from enum import Enum
from typing import Optional

from stdl.platforms.soop.types import SoopCredential


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    SOOP_LIVE = "soop_live"
    SOOP_VIDEO = "soop_video"
    TWITCH_LIVE = "twitch_live"
    YTDL_VIDEO = "ytdl_video"
    HLS_M3U8 = "hls_m3u8"


@dataclass
class ChzzkVideoRequest:
    videoNoList: list[int]
    isParallel: bool
    parallelNum: int = 3
    nonParallelDelayMs: int = 200
    cookies: Optional[str] = None


@dataclass
class ChzzkLiveRequest:
    uid: str
    cookies: Optional[str] = None


@dataclass
class SoopLiveRequest:
    userId: str
    cred: Optional[SoopCredential] = None


@dataclass
class SoopVideoRequest:
    titleNoList: list[int]
    isParallel: bool
    parallelNum: int = 3
    nonParallelDelayMs: int = 200
    cookies: Optional[str] = None


@dataclass
class TwitchLiveRequest:
    channelName: str
    cookies: Optional[str] = None


@dataclass
class YtdlVideoRequest:
    urls: list[str]


@dataclass
class HlsM3u8Request:
    urls: list[str]
    cookies: Optional[str] = None
