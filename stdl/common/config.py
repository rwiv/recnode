import json
import os

from pydantic import BaseModel

from stdl.common.requests import RequestType, \
    ChzzkLiveRequest, ChzzkVideoRequest, \
    SoopLiveRequest, TwitchLiveRequest, \
    YtdlVideoRequest, HlsM3u8Request, SoopVideoRequest
import yaml


class AppConfig(BaseModel):
    reqType: RequestType
    chzzkLive: ChzzkLiveRequest | None = None
    chzzkVideo: ChzzkVideoRequest | None = None
    soopLive: SoopLiveRequest | None = None
    soopVideo: SoopVideoRequest | None = None
    twitchLive: TwitchLiveRequest | None = None
    youtubeVideo: YtdlVideoRequest | None = None
    hlsM3u8: HlsM3u8Request | None = None
    startDelayMs: int = 0


def read_app_config_by_file(config_path: str) -> AppConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return AppConfig(**yaml.load(text, Loader=yaml.FullLoader))


def read_app_config_by_env() -> AppConfig | None:
    text = os.getenv("APP_CONFIG") or None
    if text is None:
        return None
    return AppConfig(**json.loads(text))
