from redis.asyncio import Redis

from .redis_errors import RedisError


class RedisMap:
    def __init__(self, client: Redis):
        self.__redis = client

    async def set(self, key: str, value: str, nx: bool = False, xx: bool = False, ex: int | None = None):
        result = await self.__redis.set(name=key, value=value, nx=nx, xx=xx, ex=ex)
        if result is None:
            return
        if not isinstance(result, bool):
            print(result)
            raise RedisError("Expected boolean data", 500)
        if not result:
            raise RedisError("Failed to set value", 400)

    async def get(self, key: str) -> str | None:
        result = await self.__redis.get(key)
        if result is None:
            return None
        if not isinstance(result, str):
            raise RedisError("Expected string data", 500)
        return result

    async def delete(self, key: str):
        result = await self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to delete key", 400)

    async def exists(self, key: str) -> bool:
        result = await self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result == 1
