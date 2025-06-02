import os
import sys

from .redis_errors import RedisError
from .redis_lock_pubsub import RedisPubSubLock
from .redis_lock_spin import RedisSpinLock
from .redis_queue import RedisQueue
from .redis_sorted_set import RedisSortedSet
from .redis_string import RedisString
from .redis_utils import create_redis_client, create_redis_pool, get_keys, inc_count


targets = [
    "redis_errors",
    "redis_lock_pubsub",
    "redis_lock_spin",
    "redis_queue",
    "redis_sorted_set",
    "redis_string",
    "redis_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
