import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.redis import (
    RedisString,
    get_keys,
    RedisQueue,
    create_redis_pool,
    RedisSortedSet,
)

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)

redis_str = RedisString(client)
redis_queue = RedisQueue(client)
redis_sorted_set = RedisSortedSet(client)


@pytest.mark.asyncio
async def test_get_keys():
    print()
    print(await get_keys(client))


@pytest.mark.asyncio
async def test_redis_str():
    print()
    await test_clear()
    key = "test1"
    print(await redis_str.set_expire(key, 10))
    print(await redis_str.get(key))
    print(await redis_str.contains(key))
    await redis_str.set(key, "test")
    print(await redis_str.set_expire(key, 10))
    print(await redis_str.get(key))
    print(await redis_str.contains(key))
    await redis_str.delete(key)


@pytest.mark.asyncio
async def test_redis_queue():
    print()
    await test_clear()
    key = "test2"
    print(await redis_queue.get(key))
    await redis_queue.push(key, "test1")
    await redis_queue.push(key, "test2")
    print(await redis_queue.size(key))
    print(await redis_queue.get(key))
    print(await redis_queue.pop(key))
    await redis_queue.clear(key)


@pytest.mark.asyncio
async def test_redis_sroted_set():
    print()
    await test_clear()
    key = "test3"
    await redis_sorted_set.set(key, "a", 3)
    await redis_sorted_set.set(key, "b", 5)
    await redis_sorted_set.set(key, "c", 6)
    print(await redis_sorted_set.list(key))
    print(await redis_sorted_set.size(key))
    print(await redis_sorted_set.contains_by_score(key, 2))
    print(await redis_sorted_set.contains_by_score(key, 3))
    print(await redis_sorted_set.range_by_score(key, 1, 2))
    print(await redis_sorted_set.range_by_score(key, 3, 5))
    print(await redis_sorted_set.remove_by_score(key, 2, 5))
    await redis_sorted_set.clear(key)


@pytest.mark.asyncio
async def test_clear():
    await client.delete("test1")
    await client.delete("test2")
    await client.delete("test3")
