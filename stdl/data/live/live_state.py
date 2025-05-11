from pydantic import BaseModel, Field

from ...common import PlatformType


class LiveState(BaseModel):
    id: str  # record_id
    platform: PlatformType
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")
    live_id: str = Field(alias="liveId")  # source_id
    live_title: str = Field(alias="liveTitle")
    stream_url: str = Field(alias="streamUrl")
    headers: dict[str, str] | None = None
    video_name: str = Field(alias="videoName")
    is_invalid: bool | None = Field(alias="isInvalid", default=None)
