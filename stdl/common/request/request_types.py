from enum import Enum

from pydantic import BaseModel, Field, constr


class RequestType(Enum):
    CHZZK_LIVE = "chzzk_live"
    SOOP_LIVE = "soop_live"
    TWITCH_LIVE = "twitch_live"


class ChzzkLiveRequest(BaseModel):
    uid: constr(min_length=1)
    cookies: constr(min_length=1) | None = None


class SoopLiveRequest(BaseModel):
    user_id: constr(min_length=1) = Field(alias="userId")
    cookies: constr(min_length=1) | None = None


class TwitchLiveRequest(BaseModel):
    channel_name: constr(min_length=1) = Field(alias="channelName")
    cookies: constr(min_length=1) | None = None
