from redis import Redis

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
