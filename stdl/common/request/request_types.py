from enum import Enum

from pydantic import BaseModel, Field, constr, conint


class RequestType(Enum):
    CHZZK_VIDEO = "chzzk_video"
    CHZZK_LIVE = "chzzk_live"
    SOOP_LIVE = "soop_live"
    SOOP_VIDEO = "soop_video"
    TWITCH_LIVE = "twitch_live"
    YTDL_VIDEO = "ytdl_video"
    HLS_M3U8 = "hls_m3u8"


class ChzzkVideoRequest(BaseModel):
    video_no_list: list[conint(ge=0)] = Field(alias="videoNoList")
    is_parallel: bool = Field(alias="isParallel", default=True)
    parallel_num: conint(ge=1) = Field(alias="parallelNum", default=3)
    non_parallel_delay_ms: conint(ge=0) = Field(alias="nonParallelDelayMs", default=200)
    cookies: constr(min_length=1) | None = None


class ChzzkLiveRequest(BaseModel):
    uid: constr(min_length=1)
    cookies: constr(min_length=1) | None = None


class SoopCredential(BaseModel):
    username: constr(min_length=1)
    password: constr(min_length=1)

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "password": self.password,
        }


class SoopLiveRequest(BaseModel):
    user_id: constr(min_length=1) = Field(alias="userId")
    cred: SoopCredential | None = None


class SoopVideoRequest(BaseModel):
    title_no_list: list[int] = Field(alias="titleNoList")
    is_parallel: bool = Field(alias="isParallel", default=True)
    parallel_num: conint(ge=1) = Field(alias="parallelNum", default=3)
    non_parallel_delay_ms: conint(ge=0) = Field(alias="nonParallelDelayMs", default=200)
    cookies: constr(min_length=1) | None = None


class TwitchLiveRequest(BaseModel):
    channel_name: constr(min_length=1) = Field(alias="channelName")
    cookies: constr(min_length=1) | None = None


class YtdlVideoRequest(BaseModel):
    urls: list[constr(min_length=1)]


class HlsM3u8Request(BaseModel):
    urls: list[constr(min_length=1)]
    cookies: constr(min_length=1) | None = None
