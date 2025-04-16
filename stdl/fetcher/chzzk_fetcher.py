from datetime import datetime
from typing import MutableMapping

import requests
from pydantic import BaseModel, Field

from .fetcher import LiveInfo
from ..common.spec import PlatformType


class Channel(BaseModel):
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")


class OpenLive(BaseModel):
    channel: Channel
    live_id: int = Field(alias="liveId")
    live_title: str = Field(alias="liveTitle")
    open_date: datetime = Field(alias="openDate")
    adult: bool = Field(alias="adult")

    def to_info(self) -> LiveInfo:
        return LiveInfo(
            platform=PlatformType.CHZZK,
            channel_id=self.channel.channel_id,
            channel_name=self.channel.channel_name,
            live_id=str(self.live_id),
            live_title=self.live_title,
            started_at=self.open_date,
        )


class ChzzkFetcher:
    def fetch_live_info(self, channel_id: str, headers: MutableMapping):
        url = f"https://api.chzzk.naver.com/service/v2/channels/{channel_id}/live-detail"
        res = requests.get(url, headers=headers)
        if res.status_code >= 400:
            raise ValueError(f"Failed to fetch live info: {res.status_code}")
        return OpenLive(**res.json()["content"])
