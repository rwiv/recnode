from typing import Union, Mapping

from redis.asyncio import Redis

from .redis_errors import RedisError
from .redis_utils import redis_metric


class RedisSortedSet:
    def __init__(self, client: Redis):
        self.__redis = client

    @redis_metric
    async def set_pexpire(self, key: str, px_ms: int) -> bool:  # return True if set
        return await self.__redis.pexpire(name=key, time=px_ms)

    @redis_metric
    async def set(self, key: str, value: str, score: Union[int, float, str]):  # return added count (== 1)
        return await self.__redis.zadd(key, {value: score})

    @redis_metric
    async def set_batch(self, key: str, mapping: Mapping[str, Union[int, float, str]]):  # return added count
        return await self.__redis.zadd(key, mapping=mapping)

    @redis_metric
    async def get_highest(self, key: str) -> str | None:
        lst = await self.__redis.zrevrange(key, 0, 0)
        if len(lst) == 0:
            return None
        return lst[0]

    @redis_metric
    async def get_by_score(self, key: str, score: Union[int, float, str]) -> str | None:
        lst = await self.__redis.zrangebyscore(key, score, score, start=0, num=1)
        if len(lst) == 0:
            return None
        return lst[0]

    @redis_metric
    async def range_by_score(
        self,
        key: str,
        min_score: Union[int, float, str],
        max_score: Union[int, float, str],
    ):  # return list of values
        return await self.__redis.zrangebyscore(key, min_score, max_score)

    @redis_metric
    async def list(self, key: str) -> list[str]:
        return await self.__redis.zrange(key, 0, -1)

    @redis_metric
    async def contains_by_value(self, key: str, value: str) -> bool:
        score = await self.__redis.zscore(key, value)
        return score is not None

    @redis_metric
    async def contains_by_score(self, key: str, score: Union[int, float, str]) -> bool:
        return len(await self.__redis.zrangebyscore(key, score, score, start=0, num=1)) != 0

    @redis_metric
    async def exists(self, key: str) -> bool:
        result = await self.__redis.exists(key)
        if not isinstance(result, int):
            raise RedisError("Expected integer data", 500)
        return result == 1

    @redis_metric
    async def remove_by_value(self, key: str, value: str) -> int:  # return removed count
        return await self.__redis.zrem(key, value)

    @redis_metric
    async def remove_by_score(
        self,
        key: str,
        min_score: Union[int, float, str],
        max_score: Union[int, float, str],
    ) -> int:  # return removed count
        return await self.__redis.zremrangebyscore(key, min_score, max_score)

    @redis_metric
    async def size(self, key: str) -> int:
        return await self.__redis.zcard(key)

    @redis_metric
    async def clear(self, key: str) -> int:  # return deleted count
        return await self.__redis.delete(key)
