from redis.asyncio import Redis

from .redis_errors import RedisError


class RedisQueue:
    def __init__(self, redis: Redis):
        self.__redis = redis

    async def push(self, key: str, value: str):
        await self.__redis.lpush(key, value)  # type: ignore

    async def pop(self, key: str) -> str | None:
        value = await self.__redis.rpop(key)  # type: ignore
        if value is None:
            return None
        if not isinstance(value, str):
            raise RedisError("Expected string data", 500)
        return value

    async def get(self, key: str):
        value = await self.__redis.lindex(key, -1)  # type: ignore
        if value is None:
            return None
        if not isinstance(value, str):
            raise RedisError("Expected string data", 500)
        return value

    async def get_by_index(self, key: str, idx: int) -> str | None:
        value = await self.__redis.lindex(key, idx)  # type: ignore
        if value is None:
            return None
        if not isinstance(value, str):
            raise RedisError("Expected string data", 500)
        return value

    async def list_items(self, key: str) -> list[str]:
        items = await self.__redis.lrange(key, 0, -1)  # type: ignore
        if not isinstance(items, list):
            raise RedisError("Expected list data", 500)
        return [item.decode("utf-8") for item in items]

    async def remove_by_value(self, key: str, value: str):
        await self.__redis.lrem(key, 1, value)  # type: ignore

    async def empty(self, key: str) -> bool:
        return await self.size(key) == 0

    async def size(self, key: str) -> int:
        result = await self.__redis.llen(key)  # type: ignore
        if not isinstance(result, int):
            raise RedisError("Expected int data", 500)
        return result

    async def clear(self, key: str):
        result = await self.__redis.delete(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        if result != 1:
            raise RedisError("Failed to delete key", 400)
