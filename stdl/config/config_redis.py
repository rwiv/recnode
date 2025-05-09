import os

from pydantic import BaseModel, constr, conint


class RedisConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    password: constr(min_length=1)
    ca_path: constr(min_length=1)
    pool_size: conint(ge=1)


def read_redis_config():
    return RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),  # type: ignore
        password=os.getenv("REDIS_PASSWORD"),
        ca_path=os.getenv("REDIS_CA_PATH"),
        pool_size=os.getenv("REDIS_POOL_SIZE"),  # type: ignore
    )
