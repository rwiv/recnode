import asyncio
import json
from datetime import datetime

from pydantic import BaseModel
from redis.asyncio import Redis

from .seg_num_set import SegmentNumberSet
from ..redis import RedisString, RedisPubSubLock


class SegmentState(BaseModel):
    num: int
    url: str
    duration: float
    size: int
    # parallel_limit: int
    # retry_count: int
    created_at: datetime
    updated_at: datetime

    def to_dict(self):
        return self.model_dump(mode="json", by_alias=True)


class Segment:
    def __init__(self, num: int, url: str, duration: float, limit: int):
        self.num = num
        self.url = url
        self.duration = duration
        self.limit = limit
        self.retry_count = 0

        self.is_failed = False
        self.__lock = asyncio.Lock()

    def to_dict(self, full: bool = False):
        result = {
            "num": self.num,
            "url": self.url,
            "duration": self.duration,
        }
        if full:
            result["limit"] = self.limit
            result["retry_count"] = self.retry_count
            result["is_failed"] = self.is_failed
        return result

    async def acquire(self) -> bool:
        async with self.__lock:
            if self.limit <= 0:
                return False
            self.limit -= 1
            return True

    async def release(self):
        async with self.__lock:
            self.limit += 1

    async def increment_retry_count(self):
        async with self.__lock:
            self.retry_count += 1
            return self.retry_count

    def to_new_state(self, size: int) -> SegmentState:
        return SegmentState(
            url=self.url,
            num=self.num,
            duration=self.duration,
            size=size,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )


class SegmentStateService:
    def __init__(
        self,
        client: Redis,
        live_record_id: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
        attr: dict,
    ):
        self.__client = client
        self.__str = RedisString(client)
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__attr = attr

        self.live_record_id = live_record_id

    async def renew(self, num: int):
        await self.__str.set_pexpire(self.__get_key(num), self.__expire_ms)

    async def get(self, num: int) -> SegmentState | None:
        txt = await self.__str.get(self.__get_key(num))
        if txt is None:
            return None
        return SegmentState(**json.loads(txt))

    async def set_nx(self, state: SegmentState) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True, exclude_none=True),
            nx=True,
            px=self.__expire_ms,
        )

    async def update(self, state: SegmentState) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True, exclude_none=True),
            px=self.__expire_ms,
        )

    async def delete(self, num: int) -> bool:
        return await self.__str.delete(self.__get_key(num))

    async def delete_mapped(self, nums: SegmentNumberSet):
        for num in await nums.all():
            await self.delete(num)
        await nums.clear()

    def lock(self, num: int) -> RedisPubSubLock:
        return RedisPubSubLock(
            client=self.__client,
            key=f"{self.__get_key(num)}:lock",
            expire_ms=self.__lock_expire_ms,
            timeout_sec=self.__lock_wait_timeout_sec,
        )

    def __get_key(self, num: int) -> str:
        return f"live:{self.live_record_id}:segment:{num}"
