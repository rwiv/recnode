import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.redis import RedisMap, get_keys, RedisQueue, create_redis_pool, create_redis_client

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)


@pytest.mark.asyncio
async def test_get_keys():
    print()
    print(await get_keys(client))


@pytest.mark.asyncio
async def test_redis_map():
    print()
    await test_clear()
    redis_map = RedisMap(client)
    key = "test1"
    print(await redis_map.get(key))
    print(await redis_map.exists(key))
    await redis_map.set(key, "test")
    print(await redis_map.get(key))
    print(await redis_map.exists(key))
    await redis_map.delete(key)


@pytest.mark.asyncio
async def test_redis_queue():
    print()
    await test_clear()
    redis_queue = RedisQueue(client)
    key = "test2"
    print(await redis_queue.get(key))
    await redis_queue.push(key, "test1")
    await redis_queue.push(key, "test2")
    print(await redis_queue.size(key))
    print(await redis_queue.get(key))
    print(await redis_queue.pop(key))
    await redis_queue.clear(key)


# TODO: remove this
# def test_redis_sorted_set():
#     print()
#     test_clear()
#     redis_set = RedisUniqueSortedSet(client)
#     key = "test3"
#     print(redis_set.get(key, 10))
#     print(redis_set.exists(key, 10))
#     redis_set.add(key, "test1", 10)
#     redis_set.update(key, "test10", 10)
#     redis_set.add(key, "test2", 11)
#     print(redis_set.get(key, 10))
#     print(redis_set.exists(key, 10))
#     print(redis_set.size(key))
#     print(redis_set.list(key))
#     redis_set.delete(key, 10)
#     print(redis_set.get(key, 10))
#     redis_set.clear(key)


@pytest.mark.asyncio
async def test_clear():
    await client.delete("test1")
    await client.delete("test2")
    await client.delete("test3")
