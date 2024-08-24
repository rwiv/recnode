from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    CHZZK_VID = "chzzk_vid"
    CHZZK_LIVE = "chzzk_live"


@dataclass
class ChzzkVideoRequest:
    videoNo: int


@dataclass
class ChzzkLiveRequest:
    uid: str

