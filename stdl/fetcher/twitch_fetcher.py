from datetime import datetime

from pydantic import BaseModel
from streamlink.session.session import Streamlink
from streamlink.plugin.api import validate


class TwitchLiveInfo(BaseModel):
    live_id: str
    channel_id: str
    channel_display: str
    category: str
    title: str
    created_at: datetime
    viewers_count: int


class TwitchFetcher:
    CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

    def __init__(self, session=Streamlink(), api_header=None, access_token_param=None):
        self.session = session
        self.headers = {
            "Client-ID": self.CLIENT_ID,
        }
        self.headers.update(**dict(api_header or []))
        self.access_token_params = dict(access_token_param or [])
        self.access_token_params.setdefault("playerType", "embed")

    def call(self, data, /, *, headers=None, schema, **kwargs):
        return self.session.http.post(
            "https://gql.twitch.tv/gql",
            json=data,
            headers={
                **self.headers,
                **(headers or {}),
            },
            schema=validate.Schema(
                validate.parse_json(),
                schema,
            ),
            **kwargs,
        )

    def call_raw(self, data, /, *, headers=None, **kwargs):
        return self.session.http.post(
            "https://gql.twitch.tv/gql",
            json=data,
            headers={
                **self.headers,
                **(headers or {}),
            },
            **kwargs,
        )

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

    def metadata_channel_raw(self, channel_display):
        return self.call_raw(self.metadata_channel_queries(channel_display))

    def metadata_channel(self, channel_display: str):
        schema = validate.all(
            validate.list(
                validate.all(
                    {
                        "data": {
                            "userOrError": {
                                "displayName": str,
                            },
                        },
                    },
                ),
                validate.all(
                    {
                        "data": {
                            "user": {
                                "id": str,
                                "lastBroadcast": {
                                    "title": str,
                                },
                                "stream": {
                                    "id": str,
                                    "createdAt": str,
                                    "viewersCount": int,
                                    "game": {
                                        "name": str,
                                    },
                                },
                            },
                        },
                    },
                ),
            ),
            validate.union_get(
                (1, "data", "user", "stream", "id"),
                (0, "data", "userOrError", "displayName"),
                (1, "data", "user", "stream", "game", "name"),
                (1, "data", "user", "lastBroadcast", "title"),
                (1, "data", "user", "id"),
                (1, "data", "user", "stream", "createdAt"),
                (1, "data", "user", "stream", "viewersCount"),
            ),
        )

        data = self.call(self.metadata_channel_queries(channel_display), schema=schema)
        live_id, channel_display, category, title, channel_id, created_at, viewers_count = data
        return TwitchLiveInfo(
            live_id=live_id,
            channel_id=channel_id,
            channel_display=channel_display,
            category=category,
            title=title,
            created_at=created_at,
            viewers_count=viewers_count,
        )
