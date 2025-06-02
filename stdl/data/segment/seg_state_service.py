import json
import uuid
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field
from pyutils import error_dict, log
from redis.asyncio import Redis

from .seg_num_set import SegmentNumberSet
from ..redis import RedisString, inc_count

INIT_PARALLEL_LIMIT = 1


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

    @staticmethod
    def new(url: str, num: int, duration: float, now: datetime, size: int | None = None) -> "SegmentState":
        return SegmentState(
            url=url,
            num=num,
            duration=duration,
            size=size,
            parallel_limit=INIT_PARALLEL_LIMIT,
            created_at=now,
            updated_at=now,
        )


class SegmentStateService:
    def __init__(
        self,
        master: Redis,
        replica: Redis,
        live_record_id: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
        retry_parallel_retry_limit: int,
        attr: dict,
    ):
        self.__master = master
        self.__replica = replica
        self.__str_master = RedisString(self.__master)
        self.__str_replica = RedisString(self.__replica)
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__retry_parallel_retry_limit = retry_parallel_retry_limit
        self.__attr = attr

        self.live_record_id = live_record_id

    async def get_seg(self, num: int, use_master: bool) -> SegmentState | None:
        inc_count(use_master=use_master)
        txt = await self.__get_str_redis(use_master).get(self.__get_key(num))
        if txt is None:
            return None
        return SegmentState(**json.loads(txt))

    async def get_batch(self, nums: list[int], use_master: bool) -> list[SegmentState]:
        inc_count(use_master=use_master)
        keys = [self.__get_key(num) for num in nums]
        texts = await self.__get_str_redis(use_master).mget(keys)
        result: list[SegmentState] = []
        for i, txt in enumerate(texts):
            if txt is not None:
                result.append(SegmentState(**json.loads(txt)))
            else:
                log.error("SegmentState not found", {"key": keys[i]})
        return result

    async def set_seg(self, state: SegmentState, nx: bool) -> bool:
        inc_count(use_master=True)
        return await self.__str_master.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True, exclude_none=True),
            nx=nx,
            px=self.__expire_ms,
        )

    async def set_seg_nx(self, state: SegmentState) -> bool:
        return await self.set_seg(state=state, nx=True)

    async def update_to_success(self, state: SegmentState, size: int) -> bool:
        new_state = state.copy()
        new_state.is_retrying = False
        new_state.size = size
        new_state.parallel_limit = INIT_PARALLEL_LIMIT
        new_state.updated_at = datetime.now()
        return await self.set_seg(state=new_state, nx=False)

    async def update_to_retrying(self, state: SegmentState) -> bool:
        new_state = state.copy()
        new_state.is_retrying = True
        new_state.parallel_limit = self.__retry_parallel_retry_limit
        new_state.updated_at = datetime.now()
        return await self.set_seg(state=new_state, nx=False)

    async def acquire_lock(self, state: SegmentState) -> SegmentLock | None:
        for lock_num in range(state.parallel_limit):
            lock = await self._acquire_lock(seg_num=state.num, lock_num=lock_num)
            if lock is not None:
                return lock
        return None

    async def _acquire_lock(self, seg_num: int, lock_num: int) -> SegmentLock | None:
        inc_count(use_master=True)
        token = uuid.uuid4()
        result = await self.__str_master.set(
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
        inc_count(use_master=False)
        current_token = await self.__str_replica.get(key)
        if current_token is None:
            log.debug("Lock does not exist", {"key": key})
            return
        if current_token != str(lock.token):
            raise ValueError(f"Lock Token mismatch: expected={lock.token}, actual={current_token}")
        inc_count(use_master=True)
        await self.__str_master.delete(key)

    async def is_locked(self, seg_num: int, lock_num: int, use_master: bool) -> bool:
        inc_count(use_master=use_master)
        key = self.__get_lock_key(seg_num=seg_num, lock_num=lock_num)
        return await self.__get_str_redis(use_master).exists(key)

    async def increment_retry_count(self, seg_num: int) -> int:
        inc_count(use_master=True)
        key = self.__get_retry_key(seg_num=seg_num)
        result = await self.__str_master.incr(key, px=self.__expire_ms)
        if result == 1:
            inc_count(use_master=True)
        return result

    async def get_retry_count(self, seg_num: int, use_master: bool) -> int:
        inc_count(use_master=use_master)
        key = self.__get_retry_key(seg_num=seg_num)
        count = await self.__get_str_redis(use_master).get(key)
        if count is None:
            return 0
        return int(count)

    async def clear_retry_count(self, seg_num: int):
        key = self.__get_retry_key(seg_num=seg_num)
        inc_count(use_master=False)
        if await self.__str_replica.exists(key):
            inc_count(use_master=True)
            await self.__str_master.delete(key)

    async def delete(self, num: int):
        key = self.__get_key(num)
        inc_count(use_master=False)
        if await self.__str_replica.exists(key):
            inc_count(use_master=True)
            return await self.__str_master.delete(key)

    async def delete_mapped(self, nums: SegmentNumberSet):
        for num in await nums.all(use_master=True):
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

    def __get_str_redis(self, use_master: bool = True) -> RedisString:
        return self.__str_master if use_master else self.__str_replica
