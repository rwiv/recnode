from pydantic import BaseModel, Field

from stdl.common.request_types import RequestType, ChzzkLiveRequest, SoopLiveRequest, TwitchLiveRequest
from stdl.common.types import FsType


class RecordingRequest(BaseModel):
    fs_type: FsType = Field(alias="fsType")
    req_type: RequestType = Field(alias="reqType")
    chzzk_live: ChzzkLiveRequest | None = Field(alias="chzzkLive", default=None)
    soop_live: SoopLiveRequest | None = Field(alias="soopLive", default=None)
    twitch_live: TwitchLiveRequest | None = Field(alias="twitchLive", default=None)
