from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel

from ..common.spec import PlatformType


class LiveInfo(BaseModel):
    platform: PlatformType
    channel_id: str
    channel_name: str
    live_id: str
    live_title: str
    live_started_at: datetime

    def set_dict(self, dct: dict):
        dct["platform"] = self.platform.value
        dct["channel_id"] = self.channel_id
        dct["channel_name"] = self.channel_name
        dct["live_id"] = self.live_id
        dct["live_title"] = self.live_title
        dct["live_started_at"] = self.live_started_at.isoformat()


class AbstractFetcher(ABC):

    @abstractmethod
    async def fetch_live_info(self, channel_id: str):
        pass
