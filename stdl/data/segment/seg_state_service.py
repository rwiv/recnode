import asyncio
import json
import uuid
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field
from pyutils import error_dict, log
from redis.asyncio import Redis

from .seg_num_set import SegmentNumberSet
from ..redis import RedisString


class SegmentLock(BaseModel):
    token: UUID
    seg_num: int
    lock_num: int


class SegmentState(BaseModel):
    num: int
    url: str
    duration: float
    size: int | None = Field(default=None)
    parallel_limit: int
    is_retrying: bool = Field(default=False)
    created_at: datetime
    updated_at: datetime

    def to_dict(self):
        return self.model_dump(mode="json", by_alias=True)


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
        start_time = asyncio.get_event_loop().time()
        try:
            await self.__str.set_pexpire(self.__get_key(num), self.__expire_ms)
        except Exception as ex:
            extra = {"duration": asyncio.get_event_loop().time() - start_time}
            log.error("Renew failed", self.__error_attr(ex, extra))

    async def get(self, num: int) -> SegmentState | None:
        txt = await self.__str.get(self.__get_key(num))
        if txt is None:
            return None
        return SegmentState(**json.loads(txt))

    async def get_batch(self, nums: list[int]) -> list[SegmentState]:
        keys = [self.__get_key(num) for num in nums]
        texts = await self.__str.mget(keys)
        result: list[SegmentState] = []
        for i, txt in enumerate(texts):
            if txt is not None:
                result.append(SegmentState(**json.loads(txt)))
            else:
                log.error("SegmentState not found", {"key": keys[i]})
        return result

    async def set_nx(self, state: SegmentState) -> bool:
        return await self.set(state=state, nx=True)

    async def update_to_success(self, state: SegmentState, size: int, parallel_limit: int) -> bool:
        state.is_retrying = False
        state.size = size
        state.parallel_limit = parallel_limit
        state.updated_at = datetime.now()
        return await self.set(state=state, nx=False)

    async def update_to_retrying(self, state: SegmentState, parallel_limit: int) -> bool:
        state.is_retrying = True
        state.parallel_limit = parallel_limit
        state.updated_at = datetime.now()
        return await self.set(state=state, nx=False)

    async def acquire_lock(self, state: SegmentState) -> SegmentLock | None:
        for lock_num in range(state.parallel_limit):
            lock = await self._acquire_lock(seg_num=state.num, lock_num=lock_num)
            if lock is not None:
                return lock
        return None

    async def _acquire_lock(self, seg_num: int, lock_num: int) -> SegmentLock | None:
        token = uuid.uuid4()
        result = await self.__str.set(
            key=self.__get_lock_key(seg_num=seg_num, lock_num=lock_num),
            value=str(token),
            nx=True,
            px=self.__lock_expire_ms,
        )
        if result:
            return SegmentLock(token=token, seg_num=seg_num, lock_num=lock_num)
        else:
            return None

    async def release_lock(self, lock: SegmentLock):
        key = self.__get_lock_key(seg_num=lock.seg_num, lock_num=lock.lock_num)
        current_token = await self.__str.get(key)
        if current_token is None:
            log.debug("Lock does not exist", {"key": key})
            return
        if current_token != str(lock.token):
            raise ValueError("Lock Token mismatch")
        await self.__str.delete(key)

    async def is_locked(self, seg_num: int, lock_num: int) -> bool:
        key = self.__get_lock_key(seg_num=seg_num, lock_num=lock_num)
        return await self.__str.contains(key)

    async def increment_retry_count(self, seg_num: int):
        key = self.__get_retry_key(seg_num=seg_num)
        return await self.__str.incr(key)

    async def get_retry_count(self, seg_num: int) -> int:
        key = self.__get_retry_key(seg_num=seg_num)
        count = await self.__str.get(key)
        if count is None:
            return 0
        return int(count)

    async def clear_retry_count(self, seg_num: int):
        key = self.__get_retry_key(seg_num=seg_num)
        if await self.__str.contains(key):
            await self.__str.delete(key)

    async def set(self, state: SegmentState, nx: bool) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True, exclude_none=True),
            nx=nx,
            px=self.__expire_ms,
        )

    async def delete(self, num: int) -> int:
        return await self.__str.delete(self.__get_key(num))

    async def delete_mapped(self, nums: SegmentNumberSet):
        for num in await nums.all():
            await self.delete(num)
        await nums.clear()

    def __get_key(self, num: int) -> str:
        return f"live:{self.live_record_id}:segment:{num}"

    def __get_lock_key(self, seg_num: int, lock_num: int) -> str:
        return f"{self.__get_key(seg_num)}:lock:{lock_num}"

    def __get_retry_key(self, seg_num: int) -> str:
        return f"{self.__get_key(seg_num)}:retry"

    def __error_attr(self, ex: Exception, extra: dict | None = None) -> dict:
        attr = self.__attr.copy()
        for k, v in error_dict(ex).items():
            attr[k] = v
        if extra:
            for k, v in extra.items():
                attr[k] = v
        return attr
