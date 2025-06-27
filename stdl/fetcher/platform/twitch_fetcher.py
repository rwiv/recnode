from datetime import datetime
from typing import Any

from pydantic import BaseModel
from streamlink.session.session import Streamlink

from ..fetcher import LiveInfo
from ...common import PlatformType
from ...utils import AsyncHttpClient


class TwitchLiveInfo(BaseModel):
    live_id: str
    channel_id: str
    channel_login: str
    channel_display: str
    category: str
    title: str
    created_at: datetime
    viewers_count: int

    def to_info(self) -> LiveInfo:
        return LiveInfo(
            platform=PlatformType.TWITCH,
            channel_id=self.channel_login,
            channel_name=self.channel_display,
            live_id=self.live_id,
            live_title=self.title,
        )


class TwitchFetcher:
    CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

    def __init__(self, http: AsyncHttpClient, api_header=None):
        self.__http = http
        self.__headers = {"Client-ID": self.CLIENT_ID}
        self.__headers.update(**dict(api_header or []))

    async def call(self, data, /, *, headers=None) -> Any:
        url = "https://gql.twitch.tv/gql"
        req_headers = {**self.__headers, **(headers or {})}
        return await self.__http.post_json(url=url, json=data, headers=req_headers)

    @staticmethod
    def _gql_persisted_query(operationname: str, sha256hash: str, **variables):
        return {
            "operationName": operationname,
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": sha256hash,
                },
            },
            "variables": dict(**variables),
        }

    def metadata_channel_queries(self, channel_display: str):
        return [
            self._gql_persisted_query(
                "ChannelShell",
                "c3ea5a669ec074a58df5c11ce3c27093fa38534c94286dc14b68a25d5adcbf55",
                login=channel_display,
                lcpVideosEnabled=False,
            ),
            self._gql_persisted_query(
                "StreamMetadata",
                "059c4653b788f5bdb2f5a2d2a24b0ddc3831a15079001a3d927556a96fb0517f",
                channelLogin=channel_display,
            ),
        ]

    async def metadata_channel_raw(self, channel_display, headers: dict) -> Any:
        return await self.call(self.metadata_channel_queries(channel_display), headers=headers)

    async def metadata_channel(self, channel_display, headers: dict) -> LiveInfo | None:
        data = await self.call(self.metadata_channel_queries(channel_display), headers=headers)
        if not isinstance(data, list) or len(data) != 2:
            raise ValueError("Invalid response format")

        userOrError = TwitchUserOrErrorResponse(**data[0]).data.userOrError
        user = TwitchUserResponse(**data[1]).data.user
        if user.stream is None:
            return None
        return LiveInfo(
            platform=PlatformType.TWITCH,
            channel_id=userOrError.login,
            channel_name=userOrError.displayName,
            live_id=user.stream.id,
            live_title=user.lastBroadcast.title,
        )


# userOrError
class TwitchUserOrError(BaseModel):
    id: str
    login: str
    displayName: str


class TwitchUserOrErrorData(BaseModel):
    userOrError: TwitchUserOrError


class TwitchUserOrErrorResponse(BaseModel):
    data: TwitchUserOrErrorData


# user
class TwitchUserStreamGame(BaseModel):
    id: str
    name: str


class TwitchUserStream(BaseModel):
    id: str
    createdAt: datetime
    viewersCount: int
    game: TwitchUserStreamGame


class TwitchUserLastBroadcast(BaseModel):
    id: str
    title: str


class TwitchUserUser(BaseModel):
    id: str
    lastBroadcast: TwitchUserLastBroadcast
    stream: TwitchUserStream | None


class TwitchUserData(BaseModel):
    user: TwitchUserUser


class TwitchUserResponse(BaseModel):
    data: TwitchUserData
