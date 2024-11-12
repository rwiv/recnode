import json
import os
from dataclasses import dataclass, asdict
from typing import Optional
from stdl.config.requests import RequestType, \
    ChzzkLiveRequest, ChzzkVideoRequest, \
    AfreecaLiveRequest, TwitchLiveRequest, \
    YtdlVideoRequest, HlsM3u8Request, AfreecaVideoRequest
import yaml
from dacite import from_dict


@dataclass
class AppConfig:
    reqType: str
    chzzkLive: Optional[ChzzkLiveRequest]
    chzzkVideo: Optional[ChzzkVideoRequest]
    afreecaLive: Optional[AfreecaLiveRequest]
    afreecaVideo: Optional[AfreecaVideoRequest]
    twitchLive: Optional[TwitchLiveRequest]
    youtubeVideo: Optional[YtdlVideoRequest]
    hlsM3u8: Optional[HlsM3u8Request]
    startDelayMs: int = 0

    def req_type(self) -> RequestType:
        return RequestType(self.reqType)

    def to_dict(self):
        return asdict(self)


def read_app_config_by_file(config_path: str) -> AppConfig:
    with open(config_path, "r") as file:
        text = file.read()

    d = yaml.load(text, Loader=yaml.FullLoader)
    return from_dict(data_class=AppConfig, data=d)


def read_app_config_by_env() -> Optional[AppConfig]:
    text = os.getenv("APP_CONFIG") or None
    if text is None:
        return None
    d = json.loads(text)
    return from_dict(data_class=AppConfig, data=d)
