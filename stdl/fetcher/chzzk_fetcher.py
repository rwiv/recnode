from datetime import datetime
from typing import MutableMapping

import requests
from pydantic import BaseModel, Field


class Channel(BaseModel):
    channel_id: str = Field(alias="channelId")
    channel_name: str = Field(alias="channelName")


class OpenLive(BaseModel):
    channel: Channel
    live_id: int = Field(alias="liveId")
    live_title: str = Field(alias="liveTitle")
    adult: bool = Field(alias="adult")
    open_date: datetime = Field(alias="openDate")


class ChzzkFetcher:
    def fetch_live_info(self, channel_id: str, headers: MutableMapping):
        url = f"https://api.chzzk.naver.com/service/v2/channels/{channel_id}/live-detail"
        res = requests.get(url, headers=headers)
        if res.status_code >= 400:
            raise ValueError(f"Failed to fetch live info: {res.status_code}")
        try:
            return OpenLive(**res.json()["content"])
        except KeyError:
            return None
