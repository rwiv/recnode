import json
import os

from pydantic import BaseModel, Field

from .. import FsType
from ..env.env import Env
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
import yaml


class AppConfig(BaseModel):
    fs_type: FsType | None = Field(alias="fsType", default=None)  # TODO: remove None after modifying stmgr
    req_type: RequestType = Field(alias="reqType")
    chzzk_live: ChzzkLiveRequest | None = Field(alias="chzzkLive", default=None)
    chzzk_video: ChzzkVideoRequest | None = Field(alias="chzzkVideo", default=None)
    soop_live: SoopLiveRequest | None = Field(alias="soopLive", default=None)
    soop_video: SoopVideoRequest | None = Field(alias="soopVideo", default=None)
    twitch_live: TwitchLiveRequest | None = Field(alias="twitchLive", default=None)
    youtube_video: YtdlVideoRequest | None = Field(alias="youtubeVideo", default=None)
    hls_m3u8: HlsM3u8Request | None = Field(alias="hlsM3u8", default=None)


def read_app_config_by_file(config_path: str) -> AppConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return AppConfig(**yaml.load(text, Loader=yaml.FullLoader))


def read_config(env: Env):
    conf = __read_app_config_by_env()
    if conf is None:
        conf_path = env.config_path
        if conf_path is not None:
            conf = read_app_config_by_file(conf_path)
    if conf is None:
        raise ValueError("Config not found")
    return conf


def __read_app_config_by_env() -> AppConfig | None:
    text = os.getenv("APP_CONFIG") or None
    if text is None:
        return None
    return AppConfig(**json.loads(text))
