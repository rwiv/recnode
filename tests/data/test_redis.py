from pyutils import load_dotenv, path_join, find_project_root

from stdl.common.env import get_env
from stdl.data import RedisMap, create_redis_client

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
client = create_redis_client(conf)
redis_map = RedisMap(client)


def test_redis():
    print()
    key = "test1"
    print(redis_map.get(key))
    print(redis_map.exists(key))
    redis_map.set(key, "test")
    print(redis_map.get(key))
    print(redis_map.exists(key))
    redis_map.delete(key)
