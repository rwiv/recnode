from redis.asyncio import Redis

from .redis_errors import RedisError
from .redis_utils import redis_metric


class RedisString:
    def __init__(self, client: Redis):
        self.__redis = client

    @redis_metric
    async def set_pexpire(self, key: str, px_ms: int) -> bool:  # return True if set
        return await self.__redis.pexpire(name=key, time=px_ms)

    @redis_metric
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

    @redis_metric
    async def get(self, key: str) -> str | None:
        return await self.__redis.get(key)

    @redis_metric
    async def mget(self, keys: list[str]) -> list[str | None]:
        results = await self.__redis.mget(keys=keys)
        if not isinstance(results, list):
            raise RedisError("Expected list data", 500)
        return results

    async def delete(self, key: str) -> int:  # return True if deleted
        return await self.__redis.delete(key)

    @redis_metric
    async def exists(self, key: str) -> bool:
        result = await self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result == 1

    @redis_metric
    async def incr(self, key: str, amount: int = 1, px: int | None = None) -> int:
        result = await self.__redis.incr(name=key, amount=amount)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if px is not None and result == 1:
            await self.set_pexpire(key, px)
        return result
