from dataclasses import dataclass
from enum import Enum
from typing import Optional

from stdl.platforms.afreeca.types import AfreecaCredential


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    AFREECA_LIVE = "afreeca_live"
    AFREECA_VIDEO = "afreeca_video"
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
    once: bool
    cookies: Optional[str] = None


@dataclass
class AfreecaLiveRequest:
    userId: str
    once: bool
    cred: Optional[AfreecaCredential] = None


@dataclass
class AfreecaVideoRequest:
    titleNoList: list[int]
    isParallel: bool
    parallelNum: int = 3
    nonParallelDelayMs: int = 200
    cookies: Optional[str] = None


@dataclass
class TwitchLiveRequest:
    channelName: str
    once: bool
    cookies: Optional[str] = None


@dataclass
class YtdlVideoRequest:
    urls: list[str]


@dataclass
class HlsM3u8Request:
    urls: list[str]
    cookies: Optional[str] = None
