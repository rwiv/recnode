from redis import Redis

from .redis_errors import RedisError


class RedisQueue:
    def __init__(self, redis: Redis):
        self.__redis = redis

    def push(self, key: str, value: str):
        self.__redis.lpush(key, value.encode("utf-8"))

    def pop(self, key: str) -> str | None:
        value = self.__redis.rpop(key)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data", 500)
        return value.decode("utf-8")

    def get(self, key: str):
        value = self.__redis.lindex(key, -1)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data", 500)
        return value.decode("utf-8")

    def get_by_index(self, key: str, idx: int) -> str | None:
        value = self.__redis.lindex(key, idx)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise RedisError("Expected bytes data", 500)
        return value.decode("utf-8")

    def list_items(self, key: str) -> list[str]:
        items = self.__redis.lrange(key, 0, -1)
        if not isinstance(items, list):
            raise RedisError("Expected list data", 500)
        return [item.decode("utf-8") for item in items]

    def remove_by_value(self, key: str, value: str):
        self.__redis.lrem(key, 1, value)

    def empty(self, key: str) -> bool:
        return self.size(key) == 0

    def size(self, key: str) -> int:
        result = self.__redis.llen(key)
        if not isinstance(result, int):
            raise RedisError("Expected int data", 500)
        return result

    def clear(self, key: str):
        result = self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to delete key", 400)
