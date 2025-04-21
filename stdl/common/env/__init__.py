import os
import sys

from .env import Env, get_env
from .env_redis import RedisConfig
from .env_stream import StreamConfig
from .env_proxy import ProxyConfig, read_proxy_config
from .proxy_env import ProxyEnv, get_proxy_env

targets = [
    "env",
    "env_redis",
    "env_amqp",
    "env_stream",
    "env_proxy",
    "proxy_env",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
