import os

from pydantic import BaseModel, constr, conint, confloat


class RedisConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    password: constr(min_length=1)
    ca_path: constr(min_length=1)
    pool_size_limit: conint(ge=1)


class RedisDataConfig(BaseModel):
    lock_expire_ms: conint(ge=1000)
    lock_wait_sec: confloat(ge=1)
    live_expire_sec: conint(ge=1)
    seg_expire_sec: conint(ge=1)


def read_redis_config():
    return RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),  # type: ignore
        password=os.getenv("REDIS_PASSWORD"),
        ca_path=os.getenv("REDIS_CA_PATH"),
        pool_size_limit=os.getenv("REDIS_POOL_SIZE_LIMIT"),  # type: ignore
    )


def read_redis_data_config():
    return RedisDataConfig(
        lock_expire_ms=os.getenv("REDIS_LOCK_EXPIRE_MS"),  # type: ignore
        lock_wait_sec=os.getenv("REDIS_LOCK_WAIT_SEC"),  # type: ignore
        live_expire_sec=os.getenv("REDIS_LIVE_EXPIRE_SEC"),  # type: ignore
        seg_expire_sec=os.getenv("REDIS_SEG_EXPIRE_SEC"),  # type: ignore
    )
