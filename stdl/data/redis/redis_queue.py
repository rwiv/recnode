from redis.asyncio import Redis


class RedisQueue:
    def __init__(self, redis: Redis):
        self.__redis = redis

    async def push(self, key: str, value: str):
        await self.__redis.lpush(key, value)  # type: ignore

    async def pop(self, key: str) -> str | None:
        return await self.__redis.rpop(key)  # type: ignore

    async def get(self, key: str):
        return await self.__redis.lindex(key, -1)  # type: ignore

    async def get_by_index(self, key: str, idx: int) -> str | None:
        return await self.__redis.lindex(key, idx)  # type: ignore

    async def list_items(self, key: str) -> list[str]:
        return await self.__redis.lrange(key, 0, -1)  # type: ignore

    async def remove_by_value(self, key: str, value: str):
        await self.__redis.lrem(key, 1, value)  # type: ignore

    async def empty(self, key: str) -> bool:
        return await self.size(key) == 0

    async def size(self, key: str) -> int:
        return await self.__redis.llen(key)  # type: ignore

    async def clear(self, key: str) -> int:  # deleted count
        return await self.__redis.delete(key)
