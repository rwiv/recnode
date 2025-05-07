from pyutils import load_dotenv, path_join, find_project_root

from stdl.config import get_env
from stdl.data.redis import RedisMap, create_redis_client, get_keys, RedisUniqueSortedSet, RedisQueue

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
client = create_redis_client(conf)


def test_get_keys():
    print()
    print(get_keys(client))


def test_redis_map():
    print()
    test_clear()
    redis_map = RedisMap(client)
    key = "test1"
    print(redis_map.get(key))
    print(redis_map.exists(key))
    redis_map.set(key, "test")
    print(redis_map.get(key))
    print(redis_map.exists(key))
    redis_map.delete(key)


def test_redis_queue():
    print()
    test_clear()
    redis_queue = RedisQueue(client)
    key = "test2"
    print(redis_queue.get(key))
    redis_queue.push(key, "test1")
    redis_queue.push(key, "test2")
    print(redis_queue.get(key))
    print(redis_queue.pop(key))
    redis_queue.clear(key)


def test_redis_sorted_set():
    print()
    test_clear()
    redis_set = RedisUniqueSortedSet(client)
    key = "test3"
    print(redis_set.get(key, 10))
    print(redis_set.exists(key, 10))
    redis_set.add(key, "test1", 10)
    redis_set.update(key, "test10", 10)
    redis_set.add(key, "test2", 11)
    print(redis_set.get(key, 10))
    print(redis_set.exists(key, 10))
    print(redis_set.size(key))
    print(redis_set.list(key))
    redis_set.delete(key, 10)
    print(redis_set.get(key, 10))
    redis_set.clear(key)


def test_clear():
    client.delete("test1")
    client.delete("test2")
    client.delete("test3")
