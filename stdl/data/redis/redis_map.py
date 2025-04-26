from redis import Redis, RedisError


class RedisMap:
    def __init__(self, client: Redis):
        self.__redis = client

    def set(self, key, value):
        result = self.__redis.set(key, value)
        if not isinstance(result, bool):
            raise RedisError("Expected boolean data", 422)
        if not result:
            raise RedisError("Failed to set value", 400)

    def get(self, key) -> str | None:
        result = self.__redis.get(key)
        if result is None:
            return None
        if not isinstance(result, bytes):
            raise RedisError("Expected bytes data", 422)
        return result.decode("utf-8")

    def delete(self, key):
        result = self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 422)
        if result < 0:
            raise RedisError("Failed to delete key", 400)

    def exists(self, key) -> bool:
        result = self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 422)
        return result > 0
