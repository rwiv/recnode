import os
import sys

from .config_proxy import ProxyServerConfig, read_proxy_server_config
from .config_redis import RedisConfig, RedisDataConfig
from .config_request import RequestConfig, read_request_config
from .config_stream import StreamConfig
from .env_proxy import ProxyEnv, get_proxy_env
from .env_server import Env, get_env

targets = [
    "config_proxy",
    "config_redis",
    "config_request",
    "config_stream",
    "env_proxy",
    "env_server",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
