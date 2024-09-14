from dataclasses import dataclass


@dataclass
class CdnInfo:
    cdnType: str
    zeroRating: bool


@dataclass
class Meta:
    videoId: str
    streamSeq: int
    liveId: str
    paidLive: bool
    cdnInfo: CdnInfo
    cmcdEnabled: bool
    liveRewind: bool
    duration: float


@dataclass
class EncodingTrack:
    encodingTrackId: str
    videoProfile: str
    audioProfile: str
    videoCodec: str
    videoBitRate: int
    audioBitRate: int
    videoFrameRate: str
    videoWidth: int
    videoHeight: int
    audioSamplingRate: int
    audioChannel: int
    avoidReencoding: bool
    videoDynamicRange: str


@dataclass
class Media:
    mediaId: str
    protocol: str
    path: str
    encodingTrack: list[EncodingTrack]


@dataclass
class ApiInfo:
    name: str
    path: str


@dataclass
class ChzzkPlayback:
    meta: Meta
    api: list[ApiInfo]
    media: list[Media]
