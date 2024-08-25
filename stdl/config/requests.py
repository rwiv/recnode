from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    YOUTUBE_VIDEO = "youtube_video"


@dataclass
class ChzzkVideoRequest:
    videoNoList: list[int]


@dataclass
class ChzzkLiveRequest:
    uid: str


@dataclass
class YoutubeVideoRequest:
    urls: list[str]
