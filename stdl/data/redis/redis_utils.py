from redis import Redis

from .redis_errors import RedisError
from ...common.env import RedisConfig


def create_redis_client(conf: RedisConfig) -> Redis:
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        ssl=True,
        ssl_ca_certs=conf.ca_path,
        db=0,
    )


def get_keys(client: Redis) -> list[str]:
    keys = client.keys("*")
    if not isinstance(keys, list):
        raise RedisError("Expected list data", 500)
    return [key.decode("utf-8") for key in keys]
