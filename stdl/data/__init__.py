import os
import sys

from .redis.redis_map import RedisMap
from .redis.redis_utils import create_redis_client

targets = [
    "redis",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
