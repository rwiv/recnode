import asyncio
import json
from datetime import datetime

from pydantic import BaseModel
from redis.asyncio import Redis
from streamlink.stream.hls.segment import HLSSegment

from .seg_num_set import SegmentNumberSet
from ..redis import RedisString, RedisPubSubLock


class SegmentState(BaseModel):
    url: str
    num: int
    duration: float
    size: int
    # parallel_limit: int
    # retry_count: int
    created_at: datetime
    updated_at: datetime


class SegmentStateService:
    def __init__(
        self,
        client: Redis,
        live_record_id: str,
        expire_ms: int,
        lock_expire_ms: int,
        lock_wait_timeout_sec: float,
    ):
        self.__client = client
        self.__str = RedisString(client)
        self.__live_record_id = live_record_id
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec
        self.__invalid_seg_time_diff_threshold_sec = 2 * 60  # 2 minutes
        self.__invalid_seg_num_diff_threshold = 150  # 5 minutes (150 segments == 300 seconds)

    async def renew(self, num: int):
        await self.__str.set_pexpire(self.__get_key(num), self.__expire_ms)

    async def validate_segments(self, segments: list[HLSSegment], success_nums: SegmentNumberSet) -> bool:
        if len(segments) == 0:
            raise ValueError("segments is empty")

        sorted_raw_segments = sorted(segments, key=lambda x: x.num)
        matched_nums = await success_nums.range(sorted_raw_segments[0].num, sorted_raw_segments[-1].num)
        if len(matched_nums) == 0:
            highest_num = await success_nums.get_highest()
            if highest_num is None:
                raise ValueError("No segments found in success_nums")
            return sorted_raw_segments[-1].num - highest_num > 100

        raw_segments_map = {seg.num: seg for seg in segments}
        seg_stats = await asyncio.gather(*[self.get(num) for num in matched_nums])
        filtered: list[SegmentState] = [seg for seg in seg_stats if seg is not None]
        for i, seg_stat in enumerate(filtered):
            raw_seg = raw_segments_map.get(seg_stat.num)
            if raw_seg is None:
                raise ValueError(f"Raw segment not found for num {seg_stat.num}")
            if raw_seg.uri not in seg_stat.url:
                return False
            if raw_seg.duration != seg_stat.duration:
                return False

        return False

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
