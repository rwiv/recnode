import asyncio
import random
import time
from datetime import datetime

import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis
from redis.asyncio.lock import Lock

from stdl.config import get_env
from stdl.data.redis import (
    create_redis_pool,
    RedisSpinLock,
    RedisPubSubLock,
)

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis_master
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)

ex_ms = 3_000
wt_sec = 30
ri_sec = 0.5
key = "my-resource"


async def async_func1(n: int):
    await asyncio.sleep(random.uniform(0, 10))
    lock = Lock(
        redis=client,
        name=key,
        sleep=ri_sec,
        timeout=ex_ms / 1000,
        blocking=True,
        blocking_timeout=wt_sec,
    )
    async with lock:
        print(f"{n}: {cur_time()}: Start")
        await asyncio.sleep(1)
        print(f"{n}: {cur_time()} End")


async def async_func2(n: int):
    await asyncio.sleep(random.uniform(0, 10))
    async with RedisSpinLock(client, key, expire_ms=ex_ms, timeout_sec=wt_sec, retry_sec=ri_sec):
        print(f"{n}: {cur_time()}: Start")
        await asyncio.sleep(1)
        print(f"{n}: {cur_time()} End")


async def async_func3(n: int):
    ari = 0.2
    await asyncio.sleep(random.uniform(0, 10))
    async with RedisPubSubLock(
        client=client,
        key=key,
        expire_ms=ex_ms,
        timeout_sec=wt_sec,
        auto_renew_enabled=True,
        auto_renew_interval_sec=ari,
    ):
        print(f"{n}: {cur_time()}: Start")
        await asyncio.sleep(1.5)
        print(f"{n}: {cur_time()} End")


@pytest.mark.asyncio
async def test_lock():
    print()
    start = time.time()
    coroutines = []
    for i in range(10):
        coroutines.append(async_func1(i))
        # coroutines.append(async_func2(i))
    await asyncio.gather(*coroutines)
    print(f"Time taken: {time.time() - start:.2f} seconds")


def cur_time():
    now = datetime.now()
    return now.strftime("%M:%S") + f":{int(now.microsecond / 1000):03d}"
