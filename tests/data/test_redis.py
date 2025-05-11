import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.redis import (
    RedisString,
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
async def test_redis_str():
    key = "test1"
    await test_clear()
    assert not await redis_str.set_pexpire(key, 10_000)
    assert await redis_str.get(key) is None
    assert not await redis_str.contains(key)
    assert await redis_str.set(key, "test", nx=True)
    assert not await redis_str.set(key, "test", nx=True)
    assert await redis_str.set_pexpire(key, 10_000)
    assert await redis_str.get(key) == "test"
    assert await redis_str.contains(key)
    assert await redis_str.delete(key)
    assert not await redis_str.delete(key)


@pytest.mark.asyncio
async def test_redis_queue():
    key = "test2"
    await test_clear()
    assert await redis_queue.get(key) is None
    assert await redis_queue.push(key, "test1") == 1
    assert await redis_queue.push(key, "test2") == 2
    assert await redis_queue.push(key, "test2") == 3
    assert await redis_queue.size(key) == 3
    assert await redis_queue.get(key) == "test1"
    assert await redis_queue.pop(key) == "test1"
    assert await redis_queue.get(key) == "test2"
    assert await redis_queue.clear(key) == 1


@pytest.mark.asyncio
async def test_redis_sroted_set():
    key = "test3"
    await test_clear()
    assert await redis_sorted_set.get_highest(key) is None
    assert await redis_sorted_set.set_batch(key, mapping={"a": 3, "b": 5}) == 2
    assert await redis_sorted_set.set(key, "c", 6) == 1
    assert await redis_sorted_set.list(key) == ["a", "b", "c"]
    assert await redis_sorted_set.size(key) == 3
    assert await redis_sorted_set.get_highest(key) == "c"
    assert not await redis_sorted_set.contains_by_score(key, 2)
    assert await redis_sorted_set.contains_by_score(key, 3)
    assert await redis_sorted_set.range_by_score(key, 1, 2) == []
    assert await redis_sorted_set.range_by_score(key, 3, 5) == ["a", "b"]
    assert await redis_sorted_set.remove_by_score(key, 2, 5) == 2
    assert await redis_sorted_set.clear(key) == 1


@pytest.mark.asyncio
async def test_clear():
    await client.delete("test1")
    await client.delete("test2")
    await client.delete("test3")
