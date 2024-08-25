from dataclasses import dataclass
from typing import Optional
from stdl.config.requests import RequestType, ChzzkLiveRequest, ChzzkVideoRequest, YoutubeVideoRequest
import yaml
from dacite import from_dict


@dataclass
class AppConfig:
    reqType: str
    chzzkLive: Optional[ChzzkLiveRequest]
    chzzkVideo: Optional[ChzzkVideoRequest]
    youtubeVideo: Optional[YoutubeVideoRequest]
    outDirPath: str
    cookies: Optional[str]

    def req_type(self) -> RequestType:
        return RequestType(self.reqType)


def read_app_config(config_path: str) -> AppConfig:
    with open(config_path, "r") as file:
        text = file.read()

    d = yaml.load(text, Loader=yaml.FullLoader)
    return from_dict(data_class=AppConfig, data=d)
