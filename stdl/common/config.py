import json
import os
from typing import Optional

from pydantic import BaseModel

from stdl.common.requests import RequestType, \
    ChzzkLiveRequest, ChzzkVideoRequest, \
    SoopLiveRequest, TwitchLiveRequest, \
    YtdlVideoRequest, HlsM3u8Request, SoopVideoRequest
import yaml


class AppConfig(BaseModel):
    reqType: RequestType
    chzzkLive: Optional[ChzzkLiveRequest] = None
    chzzkVideo: Optional[ChzzkVideoRequest] = None
    soopLive: Optional[SoopLiveRequest] = None
    soopVideo: Optional[SoopVideoRequest] = None
    twitchLive: Optional[TwitchLiveRequest] = None
    youtubeVideo: Optional[YtdlVideoRequest] = None
    hlsM3u8: Optional[HlsM3u8Request] = None
    startDelayMs: int = 0


def read_app_config_by_file(config_path: str) -> AppConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return AppConfig(**yaml.load(text, Loader=yaml.FullLoader))


def read_app_config_by_env() -> Optional[AppConfig]:
    text = os.getenv("APP_CONFIG") or None
    if text is None:
        return None
    return AppConfig(**json.loads(text))
