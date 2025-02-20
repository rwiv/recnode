import json
import os

import yaml
from pydantic import BaseModel, Field

from .request_types import (
    RequestType,
    ChzzkLiveRequest,
    ChzzkVideoRequest,
    SoopLiveRequest,
    TwitchLiveRequest,
    YtdlVideoRequest,
    HlsM3u8Request,
    SoopVideoRequest,
)
from ..env import Env


class AppRequest(BaseModel):
    req_type: RequestType = Field(alias="reqType")
    chzzk_live: ChzzkLiveRequest | None = Field(alias="chzzkLive", default=None)
    chzzk_video: ChzzkVideoRequest | None = Field(alias="chzzkVideo", default=None)
    soop_live: SoopLiveRequest | None = Field(alias="soopLive", default=None)
    soop_video: SoopVideoRequest | None = Field(alias="soopVideo", default=None)
    twitch_live: TwitchLiveRequest | None = Field(alias="twitchLive", default=None)
    youtube_video: YtdlVideoRequest | None = Field(alias="youtubeVideo", default=None)
    hls_m3u8: HlsM3u8Request | None = Field(alias="hlsM3u8", default=None)


def read_request_by_file(config_path: str) -> AppRequest:
    with open(config_path, "r") as file:
        text = file.read()
    return AppRequest(**yaml.load(text, Loader=yaml.FullLoader))


def read_request_by_env(env: Env):
    conf = __read_app_config_by_env()
    if conf is None:
        conf_path = env.config_path
        if conf_path is not None:
            conf = read_request_by_file(conf_path)
    if conf is None:
        raise ValueError("Config not found")
    return conf


def __read_app_config_by_env() -> AppRequest | None:
    text = os.getenv("APP_CONFIG") or None
    if text is None:
        return None
    return AppRequest(**json.loads(text))
