from redis.asyncio import Redis

from .redis_errors import RedisError


class RedisString:
    def __init__(self, client: Redis):
        self.__redis = client

    async def set_pexpire(self, key: str, px_ms: int) -> bool:  # return True if set
        return await self.__redis.pexpire(name=key, time=px_ms)

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        xx: bool = False,
        px: int | None = None,
    ) -> bool:  # return True if set
        ok = await self.__redis.set(name=key, value=value, nx=nx, xx=xx, px=px)
        if ok is None:
            return False
        if not isinstance(ok, bool):
            raise RedisError("Expected boolean data", 500)
        return ok

    async def get(self, key: str) -> str | None:
        return await self.__redis.get(key)

    async def delete(self, key: str) -> bool:  # return True if deleted
        deleted = await self.__redis.delete(key)
        if not isinstance(deleted, int):
            raise RedisError("Expected integer data", 500)
        return deleted > 0

    async def contains(self, key: str) -> bool:
        result = await self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result == 1
