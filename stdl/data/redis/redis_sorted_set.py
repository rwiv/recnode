from typing import Union

from redis.asyncio import Redis


class RedisSortedSet:
    def __init__(self, client: Redis):
        self.__redis = client

    async def set_expire(self, key: str, ex: int) -> bool:
        return await self.__redis.expire(key, ex)

    async def set(self, key: str, value: str, score: Union[int, float, str]):
        return await self.__redis.zadd(key, {value: score})  # return added count

    async def get_by_score(self, key: str, score: Union[int, float, str]) -> str:
        return await self.__redis.zrangebyscore(key, score, score, start=0, num=1)

    async def range_by_score(
        self, key: str, min_score: Union[int, float, str], max_score: Union[int, float, str]
    ):
        return await self.__redis.zrangebyscore(key, min_score, max_score)

    async def list(self, key: str) -> list[str]:
        return await self.__redis.zrange(key, 0, -1)

    async def contains_by_value(self, key: str, value: str) -> bool:
        score = await self.__redis.zscore(key, value)
        return score is not None

    async def contains_by_score(self, key: str, score: Union[int, float, str]) -> bool:
        return len(await self.__redis.zrangebyscore(key, score, score, start=0, num=1)) != 0

    async def remove_by_value(self, key: str, value: str) -> int:
        return await self.__redis.zrem(key, value)

    async def remove_by_score(
        self, key: str, min_score: Union[int, float, str], max_score: Union[int, float, str]
    ) -> int:
        return await self.__redis.zremrangebyscore(key, min_score, max_score)  # return removed count

    async def size(self, key: str) -> int:
        return await self.__redis.zcard(key)

    async def clear(self, key: str) -> int:  # deleted count
        return await self.__redis.delete(key)
