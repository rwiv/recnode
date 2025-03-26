import os
import sys

from .env import Env, get_env
from .env_amqp import AmqpConfig
from .env_stream import StreamConfig
from .env_watcher import WatcherConfig

targets = [
    "env",
    "env_amqp",
    "env_stream",
    "env_watcher",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
