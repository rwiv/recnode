from datetime import datetime
from typing import MutableMapping

import requests
from pydantic import BaseModel


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


class SoopFetcher:
    def fetch_live_info(self, user_id: str, headers: MutableMapping):
        url = f"https://chapi.sooplive.co.kr/api/{user_id}/station"
        res = requests.get(url, headers=headers)
        return SoopStationResponse(**res.json())
