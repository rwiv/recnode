from typing import Union

from redis import Redis

from .redis_errors import RedisError


class RedisUniqueSortedSet:
    def __init__(self, client: Redis):
        self.__redis = client

    def set_expire(self, key: str, ex: int):
        self.__redis.expire(key, ex)

    def add(self, key: str, value: str, score: Union[float, str]):
        if self.get(key, score):
            raise RedisError("Key already exists", 400)
        result = self.__redis.zadd(key, {value: score})
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to set value", 400)

    def update(self, key: str, value: str, score: Union[float, str]):
        self.delete(key, score)
        self.add(key, value, score)

    def get(self, key: str, score: Union[float, str]) -> str | None:
        result = self.__redis.zrangebyscore(key, score, score)
        if not isinstance(result, list):
            raise RedisError("Expected list data", 500)
        if not result:
            return None
        if len(result) != 1:
            raise RedisError("Expected one result", 400)
        data = result[0]
        if not isinstance(data, bytes):
            raise RedisError("Expected bytes data", 500)
        return data.decode("utf-8")

    def list(self, key: str) -> list[str]:
        result = self.__redis.zrange(key, 0, -1)
        if not isinstance(result, list):
            raise RedisError("Expected list data", 500)
        return [item.decode("utf-8") for item in result]

    def delete(self, key: str, score: Union[float, str]):
        result = self.__redis.zremrangebyscore(key, score, score)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to delete key", 400)

    def size(self, key: str) -> int:
        result = self.__redis.zcard(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result

    def exists(self, key: str, score: Union[float, str]) -> bool:
        result = self.__redis.zrangebyscore(key, score, score)
        if not isinstance(result, list):
            raise RedisError("Expected list data", 500)
        if len(result) == 0:
            return False
        if len(result) != 1:
            raise RedisError("Expected one result", 400)
        return True

    def clear(self, key: str):
        result = self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to delete key", 400)
