import asyncio
import time
from datetime import datetime

import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.redis import (
    create_redis_pool,
    RedisSpinLock,
    RedisPubSubLock,
)

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)


async def async_func1(n: int):
    ex = 3_000
    wt = 30_000
    ri = 0.1
    async with RedisSpinLock(client, "my-resource", expire_sec=ex, wait_timeout_ms=wt, retry_interval=ri):
        print(f"{n}: {cur_time()}: Start")
        await asyncio.sleep(1)
        print(f"{n}: {cur_time()} End")


async def async_func2(n: int):
    # ex_ms = 3_000
    ex_ms = 500
    wt_sec = 30
    ari = 0.2
    key = "my-resource"

    async with RedisPubSubLock(
        client=client,
        key=key,
        expire_ms=ex_ms,
        timeout_sec=wt_sec,
        auto_renew_enabled=True,
        auto_renew_interval_sec=ari,
    ):
        print(f"{n}: {cur_time()}: Start")
        await asyncio.sleep(1)
        print(f"{n}: {cur_time()} End")


@pytest.mark.asyncio
async def test_lock():
    print()
    start = time.time()
    coroutines = []
    for i in range(20):
        # coroutines.append(async_func1(i))
        coroutines.append(async_func2(i))
        await asyncio.sleep(0.1)
    await asyncio.gather(*coroutines)
    print(f"Time taken: {time.time() - start:.2f} seconds")


def cur_time():
    now = datetime.now()
    return now.strftime("%M:%S") + f":{int(now.microsecond / 1000):03d}"
