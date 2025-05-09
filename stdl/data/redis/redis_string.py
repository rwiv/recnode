from redis.asyncio import Redis

from .redis_errors import RedisError


class RedisString:
    def __init__(self, client: Redis):
        self.__redis = client

    async def set_expire(self, key: str, ex: int) -> bool:
        return await self.__redis.expire(key, ex)

    async def set(self, key: str, value: str, nx: bool = False, xx: bool = False, ex: int | None = None):
        result = await self.__redis.set(name=key, value=value, nx=nx, xx=xx, ex=ex)
        if result is None:
            return
        if not result:
            raise RedisError("Failed to set value", 400)

    async def get(self, key: str) -> str | None:
        return await self.__redis.get(key)

    async def delete(self, key: str):
        await self.__redis.delete(key)

    async def contains(self, key: str) -> bool:
        result = await self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result == 1
