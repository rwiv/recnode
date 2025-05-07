from datetime import datetime

import aiohttp
from pydantic import BaseModel, Field

from ..fetcher import LiveInfo
from ...common import PlatformType
from ...utils import HttpRequestError


class ChzzkChannel(BaseModel):
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")


class ChzzkLive(BaseModel):
    channel: ChzzkChannel
    live_id: int = Field(alias="liveId")
    live_title: str = Field(alias="liveTitle")
    open_date: datetime = Field(alias="openDate")
    close_date: datetime | None = Field(alias="closeDate")
    adult: bool = Field(alias="adult")

    def to_info(self) -> LiveInfo:
        return LiveInfo(
            platform=PlatformType.CHZZK,
            channel_id=self.channel.channel_id,
            channel_name=self.channel.channel_name,
            live_id=str(self.live_id),
            live_title=self.live_title,
        )


class ChzzkFetcher:
    async def fetch_live_info(self, channel_id: str, headers: dict) -> LiveInfo | None:
        url = f"https://api.chzzk.naver.com/service/v2/channels/{channel_id}/live-detail"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers) as res:
                if res.status >= 400:
                    raise HttpRequestError("Failed to request", res.status, url, res.method, res.reason)
                data = await res.json()
                info = ChzzkLive(**data["content"])
                if info.close_date is not None:
                    return None
                return info.to_info()
