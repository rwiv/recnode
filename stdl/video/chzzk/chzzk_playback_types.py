from pydantic import BaseModel


# class EncodingTrack(BaseModel):
#     encodingTrackId: str
#     videoProfile: str
#     audioProfile: str
#     videoCodec: str
#     videoBitRate: int
#     audioBitRate: int
#     videoFrameRate: str
#     videoWidth: int
#     videoHeight: int
#     audioSamplingRate: int
#     audioChannel: int
#     avoidReencoding: bool
#     videoDynamicRange: str


class Media(BaseModel):
    # mediaId: str
    # protocol: str
    path: str
    # encodingTrack: list[EncodingTrack]


# class CdnInfo(BaseModel):
#     cdnType: str
#     zeroRating: bool
#
#
# class Meta(BaseModel):
#     videoId: str
#     streamSeq: int
#     liveId: str
#     paidLive: bool
#     cdnInfo: CdnInfo
#     cmcdEnabled: bool
#     liveRewind: bool
#     duration: float
#
#
# class ApiInfo(BaseModel):
#     name: str
#     path: str


class ChzzkPlayback(BaseModel):
    # meta: Meta
    # api: list[ApiInfo]
    media: list[Media]
