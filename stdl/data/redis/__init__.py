import os
import sys

from .redis_map import RedisMap
from .redis_errors import RedisError
from .redis_us_set import RedisUniqueSortedSet
from .redis_queue import RedisQueue
from .redis_utils import create_redis_client, get_keys

targets = [
    "redis_map",
    "redis_errors",
    "redis_sorted_set",
    "redis_queue",
    "redis_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
