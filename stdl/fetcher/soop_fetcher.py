from datetime import datetime

import aiohttp
from pydantic import BaseModel

from .fetcher import LiveInfo
from ..common.spec import PlatformType
from ..utils import HttpRequestError


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

    def to_info(self) -> LiveInfo | None:
        if self.broad is None:
            return None
        return LiveInfo(
            platform=PlatformType.SOOP,
            channel_id=self.station.user_id,
            channel_name=self.station.user_nick,
            live_id=str(self.broad.broad_no),
            live_title=self.broad.broad_title,
        )


class SoopFetcher:
    async def fetch_live_info(self, user_id: str, headers: dict) -> LiveInfo | None:
        url = f"https://chapi.sooplive.co.kr/api/{user_id}/station"
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers) as res:
                if res.status >= 400:
                    raise HttpRequestError("Failed to request", res.status, url, res.method, res.reason)
                return SoopStationResponse(**await res.json()).to_info()
