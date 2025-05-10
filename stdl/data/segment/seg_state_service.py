import asyncio
import json
from datetime import datetime

from pydantic import BaseModel
from redis.asyncio import Redis

from .seg_num_set import SegmentNumberSet
from ..redis import RedisString, RedisPubSubLock
from ...utils import AsyncHttpClient


class SegmentState(BaseModel):
    url: str
    num: int
    duration: float
    size: int
    # parallel_limit: int
    # retry_count: int
    created_at: datetime
    updated_at: datetime


class Segment:
    def __init__(self, num: int, url: str, duration: float, limit: int):
        self.num = num
        self.url = url
        self.duration = duration
        self.limit = limit
        self.retry_count = 0

        self.is_failed = False
        self.__lock = asyncio.Lock()

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
        seg_http: AsyncHttpClient,
    ):
        self.__client = client
        self.__str = RedisString(client)
        self.__live_record_id = live_record_id
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__invalid_seg_time_diff_threshold_sec = 2 * 60  # 2 minutes
        self.__invalid_seg_num_diff_threshold = 150  # 5 minutes (150 segments == 300 seconds)
        self.__seg_http = seg_http

    async def renew(self, num: int):
        await self.__str.set_pexpire(self.__get_key(num), self.__expire_ms)

    async def validate_segments(self, segments: list[Segment], success_nums: SegmentNumberSet) -> bool:
        if len(segments) == 0:
            raise ValueError("segments is empty")

        sorted_raw_segments = sorted(segments, key=lambda x: x.num)
        matched_nums = await success_nums.range(sorted_raw_segments[0].num, sorted_raw_segments[-1].num)
        if len(matched_nums) == 0:
            highest_num = await success_nums.get_highest()
            if highest_num is None:
                raise ValueError("No segments found in success_nums")
            return sorted_raw_segments[-1].num - highest_num > 100

        res = await asyncio.gather(*[self.get(num) for num in matched_nums])
        seg_stats: list[SegmentState] = sorted([seg for seg in res if seg is not None], key=lambda x: x.num)
        segments_map = {seg.num: seg for seg in segments}
        for i, seg_stat in enumerate(seg_stats):
            seg = segments_map.get(seg_stat.num)
            if seg is None:
                raise ValueError(f"Raw segment not found for num {seg_stat.num}")
            if seg.url != seg_stat.url:
                return False
            if seg.duration != seg_stat.duration:
                return False
            if i == len(seg_stats) - 1:
                b = await self.__seg_http.get_bytes(url=seg.url)
                if len(b) != seg_stat.size:
                    return False

        return True

    async def validate_segment(self, num: int, success_nums: SegmentNumberSet) -> tuple[bool, bool]:
        if not await success_nums.get(num):
            return True, False
        seg = await self.get(num)
        if seg is None:
            raise ValueError(f"Segment {num} not found")
        if self.__is_invalid_seg(seg):
            return False, True
        else:
            return False, False

    async def __is_invalid_seg(self, seg: SegmentState) -> bool:
        diff = datetime.now() - seg.created_at
        return diff.seconds > self.__invalid_seg_time_diff_threshold_sec

    async def get(self, num: int) -> SegmentState | None:
        txt = await self.__str.get(self.__get_key(num))
        if txt is None:
            return None
        return SegmentState(**json.loads(txt))

    async def set_nx(self, state: SegmentState) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True),
            nx=True,
            px=self.__expire_ms,
        )

    async def update(self, state: SegmentState) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True),
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
        return f"live:{self.__live_record_id}:segment:{num}"
