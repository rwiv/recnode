from datetime import datetime
from typing import MutableMapping

import requests
from pydantic import BaseModel

from .fetcher import LiveInfo
from ..common.spec import PlatformType


class SoopStation(BaseModel):
    user_id: str
    user_nick: str
    broad_start: datetime


class SoopBroad(BaseModel):
    broad_no: int
    broad_title: str
    current_sum_viewer: int
    is_password: bool
    broad_grade: int


class SoopStationResponse(BaseModel):
    station: SoopStation
    broad: SoopBroad | None

    def to_info(self) -> LiveInfo:
        if self.broad is None:
            raise ValueError("No live info available")
        return LiveInfo(
            platform=PlatformType.SOOP,
            channel_id=self.station.user_id,
            channel_name=self.station.user_nick,
            live_id=str(self.broad.broad_no),
            live_title=self.broad.broad_title,
            started_at=self.station.broad_start,
        )


class SoopFetcher:
    def fetch_live_info(self, user_id: str, headers: MutableMapping):
        url = f"https://chapi.sooplive.co.kr/api/{user_id}/station"
        res = requests.get(url, headers=headers)
        return SoopStationResponse(**res.json())
