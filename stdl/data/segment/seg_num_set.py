from redis.asyncio import Redis

from ..redis import RedisSortedSet, RedisPubSubLock


class SegmentNumberSet:
    def __init__(
        self,
        client: Redis,
        live_record_id: str,
        key_suffix: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
    ):
        self.__client = client
        self.__sorted_set = RedisSortedSet(client)
        self.__live_record_id = live_record_id
        self.__key_suffix = key_suffix
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec

    async def renew(self):
        await self.__sorted_set.set_pexpire(self.__get_key(), self.__expire_ms)

    async def set(self, num: int):
        await self.__sorted_set.set(self.__get_key(), str(num), num)

    def new_lock(self) -> RedisPubSubLock:
        return RedisPubSubLock(
            client=self.__client,
            key=f"{self.__get_key()}:lock",
            expire_ms=self.__lock_expire_ms,
            timeout_sec=self.__lock_wait_timeout_sec,
        )

    async def all(self) -> list[int]:
        result = await self.__sorted_set.list(self.__get_key())
        return [int(i) for i in result]

    async def range(self, start: int, end: int) -> list[int]:
        result = await self.__sorted_set.range_by_score(self.__get_key(), start, end)
        return [int(i) for i in result]

    async def remove(self, num: int):
        await self.__sorted_set.remove_by_score(self.__get_key(), num, num)

    async def contains(self, num: int) -> bool:
        return await self.__sorted_set.contains_by_score(self.__get_key(), num)

    async def size(self) -> int:
        return await self.__sorted_set.size(self.__get_key())

    async def clear(self):
        await self.__sorted_set.clear(self.__get_key())

    def __get_key(self):
        return f"live:{self.__live_record_id}:segments:{self.__key_suffix}"
