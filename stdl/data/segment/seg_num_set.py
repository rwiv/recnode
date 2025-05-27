import asyncio

from pyutils import log, error_dict
from redis.asyncio import Redis

from ..redis import RedisSortedSet, RedisSpinLock


LOCK_RETRY_INTERVAL_SEC = 0.1


class SegmentNumberSet:
    def __init__(
        self,
        client: Redis,
        live_record_id: str,
        key_suffix: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
        attr: dict,
    ):
        self.__client = client
        self.__sorted_set = RedisSortedSet(client)
        self.__live_record_id = live_record_id
        self.__key_suffix = key_suffix
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__attr = attr

    async def renew(self):
        start_time = asyncio.get_event_loop().time()
        try:
            await self.__sorted_set.set_pexpire(self.__get_key(), self.__expire_ms)
        except Exception as ex:
            extra = {"duration": asyncio.get_event_loop().time() - start_time}
            log.error("Renew failed", self.__error_attr(ex, extra))

    async def set(self, num: int):
        await self.__sorted_set.set(self.__get_key(), str(num), num)

    async def all(self) -> list[int]:
        result = await self.__sorted_set.list(self.__get_key())
        return [int(i) for i in result]

    async def get(self, num: int) -> int | None:
        result = await self.__sorted_set.get_by_score(self.__get_key(), num)
        if result is None:
            return None
        return int(result)

    async def get_highest(self) -> int | None:
        result = await self.__sorted_set.get_highest(self.__get_key())
        if result is None:
            return None
        return int(result)

    async def range(self, start: int, end: int) -> list[int]:
        result = await self.__sorted_set.range_by_score(self.__get_key(), start, end)
        return [int(i) for i in result]

    async def remove(self, num: int):
        if await self.contains(num):
            await self.__sorted_set.remove_by_value(self.__get_key(), str(num))

    async def contains(self, num: int) -> bool:
        return await self.__sorted_set.contains_by_value(self.__get_key(), str(num))

    async def size(self) -> int:
        return await self.__sorted_set.size(self.__get_key())

    async def clear(self):
        await self.__sorted_set.clear(self.__get_key())

    def lock(self) -> RedisSpinLock:
        return RedisSpinLock(
            client=self.__client,
            key=f"{self.__get_key()}:lock",
            expire_ms=self.__lock_expire_ms,
            timeout_sec=self.__lock_wait_timeout_sec,
            retry_sec=LOCK_RETRY_INTERVAL_SEC,
        )

    def __get_key(self):
        return f"live:{self.__live_record_id}:segments:{self.__key_suffix}"

    def __error_attr(self, ex: Exception, extra: dict | None = None):
        attr = self.__attr.copy()
        for k, v in error_dict(ex).items():
            attr[k] = v
        if extra:
            for k, v in extra.items():
                attr[k] = v
        return attr
