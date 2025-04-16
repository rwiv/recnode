from abc import ABC, abstractmethod

from pydantic import BaseModel

from ..common.spec import PlatformType


class LiveInfo(BaseModel):
    platform: PlatformType
    channel_id: str
    live_id: str
    title: str


class AbstractFetcher(ABC):

    @abstractmethod
    def fetch_live_info(self, channel_id: str):
        pass
