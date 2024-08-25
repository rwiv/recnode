from dataclasses import dataclass
from typing import Optional


@dataclass
class Channel:
    channelId: str
    channelName: str
    channelImageUrl: str
    verifiedMark: bool


@dataclass
class Video:
    videoNo: int
    videoId: str
    videoTitle: str
    videoType: str
    publishDate: str
    thumbnailImageUrl: str
    trailerUrl: Optional[str]
    duration: int
    readCount: int
    publishDateAt: int
    categoryType: str
    videoCategory: str
    videoCategoryValue: str
    exposure: bool
    adult: bool
    clipActive: bool
    channel: Channel
    blindType: Optional[str]


@dataclass
class AdParameter:
    tag: str


@dataclass
class Content(Video):
    paidPromotion: bool
    inKey: str
    liveOpenDate: Optional[str]
    vodStatus: str
    prevVideo: Optional[Video]
    nextVideo: Optional[Video]
    userAdultStatus: Optional[str]
    adParameter: AdParameter


@dataclass
class ChzzkVideoResponse:
    code: int
    message: Optional[str]
    content: Content
