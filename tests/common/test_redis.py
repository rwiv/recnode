from pyutils import load_dotenv, path_join, find_project_root

from stdl.common.env import get_env
from stdl.data.redis import RedisClient

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
redis = RedisClient(conf)


def test_redis():
    print()
    a = redis.get("test1")
    print(a)
