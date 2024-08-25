from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    YTDL_VIDEO = "ytdl_video"
    AFREECA_LIVE = "afreeca_live"


@dataclass
class ChzzkVideoRequest:
    videoNoList: list[int]


@dataclass
class ChzzkLiveRequest:
    uid: str


@dataclass
class AfreecaLiveRequest:
    userId: str


@dataclass
class YtdlVideoRequest:
    urls: list[str]
