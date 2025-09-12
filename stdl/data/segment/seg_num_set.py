import asyncio

from pyutils import log, error_dict
from redis.asyncio import Redis
from redis.asyncio.lock import Lock

from ..redis import RedisSortedSet, inc_count

LOCK_RETRY_INTERVAL_SEC = 0.1
RENEW_THRESHOLD_MS = 60 * 1000  # 1 minute


class SegmentNumberSet:
    def __init__(
        self,
        master: Redis,
        replica: Redis,
        live_record_id: str,
        key_suffix: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
        attr: dict,
    ):
        self.__master = master
        self.__replica = replica
        self.__sorted_set_master = RedisSortedSet(self.__master)
        self.__sorted_set_replica = RedisSortedSet(self.__replica)
        self.__live_record_id = live_record_id
        self.__key_suffix = key_suffix
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__attr = attr

    async def renew(self):
        key = self.__get_key()
        inc_count(use_master=False)
        ttl = await self.__replica.pttl(key)
        if ttl is None or ttl == -2 or ttl > RENEW_THRESHOLD_MS:
            return

        start_time = asyncio.get_event_loop().time()
        inc_count(use_master=True)
        try:
            await self.__sorted_set_master.set_pexpire(key, self.__expire_ms)
        except Exception as ex:
            extra = {"duration": asyncio.get_event_loop().time() - start_time}
            log.error("Renew failed", self.__error_attr(ex, extra))

    async def set_num(self, num: int):
        inc_count(use_master=True)
        await self.__sorted_set_master.set(self.__get_key(), str(num), num)

    async def all(self, use_master: bool) -> list[int]:
        inc_count(use_master=use_master)
        result = await self.__get_sorted_set(use_master).list(self.__get_key())
        return [int(i) for i in result]

    async def get_highest(self, use_master: bool) -> int | None:
        inc_count(use_master=use_master)
        result = await self.__get_sorted_set(use_master).get_highest(self.__get_key())
        if result is None:
            return None
        return int(result)

    async def range(self, start: int, end: int, use_master: bool) -> list[int]:
        inc_count(use_master=use_master)
        result = await self.__get_sorted_set(use_master).range_by_score(self.__get_key(), start, end)
        return [int(i) for i in result]

    async def remove(self, num: int, check_replica: bool = True):
        if check_replica:
            inc_count(use_master=False)
            if not await self.contains(num, use_master=False):
                return
        inc_count(use_master=False)
        # If replica check is performed, data might not be deleted
        await self.__sorted_set_master.remove_by_value(self.__get_key(), str(num))

    async def contains(self, num: int, use_master: bool) -> bool:
        inc_count(use_master=use_master)
        return await self.__get_sorted_set(use_master).contains_by_value(self.__get_key(), str(num))

    async def size(self, use_master: bool) -> int:
        inc_count(use_master=use_master)
        return await self.__get_sorted_set(use_master).size(self.__get_key())

    async def clear(self):
        inc_count(use_master=True)
        await self.__sorted_set_master.clear(self.__get_key())

    def lock(self) -> Lock:
        inc_count(use_master=True, amount=2)
        return Lock(
            redis=self.__master,
            name=f"{self.__get_key()}:lock",
            sleep=LOCK_RETRY_INTERVAL_SEC,
            timeout=self.__lock_expire_ms / 1000,
            blocking=True,
            blocking_timeout=self.__lock_wait_timeout_sec,
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

    def __get_sorted_set(self, use_master: bool) -> RedisSortedSet:
        return self.__sorted_set_master if use_master else self.__sorted_set_replica
