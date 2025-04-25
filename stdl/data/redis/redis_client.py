from redis import Redis

from ...common.env import RedisConfig


class RedisClient:
    def __init__(self, conf: RedisConfig):
        self.__redis = Redis(
            host=conf.host,
            port=conf.port,
            password=conf.password,
            ssl=True,
            ssl_ca_certs=conf.ca_path,
            db=0,
        )

    def set(self, key, value):
        return self.__redis.set(key, value)

    def get(self, key):
        return self.__redis.get(key)

    def delete(self, key):
        return self.__redis.delete(key)

    def exists(self, key):
        return self.__redis.exists(key)
