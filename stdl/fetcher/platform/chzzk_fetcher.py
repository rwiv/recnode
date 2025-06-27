from datetime import datetime

from pydantic import BaseModel, Field

from ..fetcher import LiveInfo
from ...common import PlatformType
from ...utils import AsyncHttpClient


class ChzzkChannelInfo(BaseModel):
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")
    open_live: bool = Field(alias="openLive")


class ChzzkOpenLive(BaseModel):
    channel_id: str = Field(alias="channelId")
    live_id: int = Field(alias="liveId")
    live_title: str = Field(alias="liveTitle")
    open_date: datetime = Field(alias="openDate")
    adult: bool = Field(alias="adult")

    def to_info(self, channel_name: str) -> LiveInfo:
        return LiveInfo(
            platform=PlatformType.CHZZK,
            channel_id=self.channel_id,
            channel_name=channel_name,
            live_id=str(self.live_id),
            live_title=self.live_title,
        )


class ChzzkFetcher:
    def __init__(self, http: AsyncHttpClient):
        self.__http = http

    async def fetch_live_info(self, channel_id: str, headers: dict) -> LiveInfo | None:
        url = f"https://api.chzzk.naver.com/service/v1/channels/{channel_id}"
        data = await self.__http.get_json(url=url, headers=headers)
        content = data.get("content")
        if content is None:
            return None
        channel_info = ChzzkChannelInfo(**content)
        if not channel_info.open_live:
            return None

        url = f"https://api.chzzk.naver.com/service/v1/channels/{channel_id}/data?fields=topExposedVideos"
        data = await self.__http.get_json(url=url, headers=headers)
        content = data.get("content")
        if content is None:
            return None
        top_exposed_videos = content.get("topExposedVideos")
        if top_exposed_videos is None:
            return None
        open_live = top_exposed_videos.get("openLive")
        if open_live is None:
            return None
        return ChzzkOpenLive(**open_live).to_info(channel_info.channel_name)
