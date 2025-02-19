from enum import Enum

from pydantic import BaseModel, Field

from stdl.record import SoopCredential


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    SOOP_LIVE = "soop_live"
    SOOP_VIDEO = "soop_video"
    TWITCH_LIVE = "twitch_live"
    YTDL_VIDEO = "ytdl_video"
    HLS_M3U8 = "hls_m3u8"


class ChzzkVideoRequest(BaseModel):
    video_no_list: list[int] = Field(alias="videoNoList")
    is_parallel: bool = Field(alias="isParallel")
    parallel_num: int = Field(alias="parallelNum", default=3)
    non_parallel_delay_ms: int = Field(alias="nonParallelDelayMs", default=200)
    cookies: str | None = Field(min_length=1, default=None)


class ChzzkLiveRequest(BaseModel):
    uid: str = Field(min_length=1)
    cookies: str | None = Field(min_length=1, default=None)


class SoopLiveRequest(BaseModel):
    user_id: str = Field(min_length=1, alias="userId")
    cred: SoopCredential | None = Field(min_length=1, default=None)


class SoopVideoRequest(BaseModel):
    title_no_list: list[int] = Field(alias="titleNoList")
    is_parallel: bool = Field(alias="isParallel")
    parallel_num: int = Field(alias="parallelNum", default=3)
    non_parallel_delay_ms: int = Field(alias="nonParallelDelayMs", default=200)
    cookies: str | None = Field(min_length=1, default=None)


class TwitchLiveRequest(BaseModel):
    channel_name: str = Field(min_length=1, alias="channelName")
    cookies: str | None = Field(min_length=1, default=None)


class YtdlVideoRequest(BaseModel):
    urls: list[str]


class HlsM3u8Request(BaseModel):
    urls: list[str]
    cookies: str | None = Field(min_length=1, default=None)
