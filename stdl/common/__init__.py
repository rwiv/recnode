import sys

from .amqp.amqp import AmqpHelper, AmqpHelperBlocking, AmqpHelperMock
from .amqp.amqp_utils import create_amqp
from .spec.common_types import PlatformType, FsType
from .env.env import Env, get_env
from .fs.fs_config import FsConfig, S3Config
from .fs.fs_config_utils import read_fs_config_by_file, create_fs_accessor
from .request.request_config import AppConfig, read_config, read_app_config_by_file
from .request.request_types import (
    RequestType,
    ChzzkVideoRequest,
    ChzzkLiveRequest,
    SoopLiveRequest,
    SoopVideoRequest,
    TwitchLiveRequest,
    YtdlVideoRequest,
    HlsM3u8Request,
)

targets = ["amqp", "env", "fs", "request"]
for name in list(sys.modules.keys()):
    for target in targets:
        if name.startswith(f"{__name__}.{target}"):
            sys.modules[name] = None  # type: ignore
