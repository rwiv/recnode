from redis.asyncio import Redis, ConnectionPool, SSLConnection

from ...config import RedisConfig


def create_redis_pool(conf: RedisConfig) -> ConnectionPool:
    return ConnectionPool(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=0,
        decode_responses=True,
        connection_class=SSLConnection,
        ssl_ca_certs=conf.ca_path,
        max_connections=conf.pool_size,
    )


def create_redis_client(conf: RedisConfig) -> Redis:
    return Redis(
        host=conf.host,
        port=conf.port,
        password=conf.password,
        db=0,
        decode_responses=True,
        ssl=True,
        ssl_ca_certs=conf.ca_path,
    )


async def get_keys(client: Redis, match: str = "*", cnt: int = 100) -> list[str]:
    cursor = 0
    keys = []
    while True:
        cursor, cur_keys = await client.scan(cursor=cursor, match=match, count=cnt)
        keys.extend(cur_keys)
        if cursor == 0:
            break
    return keys
