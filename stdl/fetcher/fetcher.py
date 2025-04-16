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
    started_at: datetime


class AbstractFetcher(ABC):

    @abstractmethod
    def fetch_live_info(self, channel_id: str):
        pass
