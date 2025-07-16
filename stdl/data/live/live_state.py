from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from ...common import PlatformType


class LocationType(Enum):
    LOCAL = "local"
    PROXY_DOMESTIC = "proxy_domestic"
    PROXY_OVERSEAS = "proxy_overseas"


class LiveState(BaseModel):
    id: str  # record_id
    platform: PlatformType
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")
    live_id: str = Field(alias="liveId")  # source_id
    live_title: str = Field(alias="liveTitle")
    stream_url: str = Field(alias="streamUrl")
    headers: dict[str, str] | None = None  # change to `streamHeaders`
    platform_cookie: str | None = Field(alias="platformCookie", default=None)
    video_name: str = Field(alias="videoName")
    is_invalid: bool = Field(alias="isInvalid")
    location: LocationType
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
