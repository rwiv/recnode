import asyncio
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pyutils import log, error_dict

from .seg_num_set import SegmentNumberSet
from .seg_state_service import SegmentState, Segment, SegmentStateService
from ..live import LiveStateService
from ...utils import AsyncHttpClient


class SegmentInspect(BaseModel):
    ok: bool
    critical: bool
    attr: dict[str, Any] = {}

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SegmentInspect):
            return self.ok == other.ok and self.critical == other.critical
        return False


class SegmentStateValidator:
    def __init__(
        self,
        live_state_service: LiveStateService,
        seg_state_service: SegmentStateService,
        seg_http: AsyncHttpClient,
        attr: dict,
        invalid_seg_time_diff_threshold_sec=2 * 60,  # 2 minutes
        invalid_seg_num_diff_threshold=150,  # 5 minutes (150 segments == 300 seconds)
        req_retry_limit=2,
    ):
        self.__live_state_service = live_state_service
        self.__seg_state_service = seg_state_service
        self.__seg_http = seg_http
        self.__attr = attr
        self.__invalid_seg_time_diff_threshold_sec = invalid_seg_time_diff_threshold_sec
        self.__invalid_seg_num_diff_threshold = invalid_seg_num_diff_threshold
        self.__req_retry_limit = req_retry_limit

    async def validate_segments(self, req_segments: list[Segment], success_nums: SegmentNumberSet) -> bool:
        try:
            log.debug("Validate segments", self.__attr)
            if len(req_segments) == 0:
                log.error("segments is empty", self.__attr)
                return False

            latest_num = await success_nums.get_highest()
            if latest_num is None:
                return True  # init recording

            sorted_req_segments = sorted(req_segments, key=lambda x: x.num)
            matched_nums = await success_nums.range(sorted_req_segments[0].num, sorted_req_segments[-1].num)
            if len(matched_nums) == 0:
                highest_num = await success_nums.get_highest()
                if highest_num is None:
                    log.error("No segments found in success_nums", self.__attr)
                    return False
                if sorted_req_segments[-1].num - highest_num > self.__invalid_seg_num_diff_threshold:
                    return await self.__update_to_invalid_live()

            matched_req_segments = [seg for seg in sorted_req_segments if seg.num in matched_nums]
            res = await asyncio.gather(*[self.__seg_state_service.get(num) for num in matched_nums])
            matched_seg_states: list[SegmentState] = sorted([seg for seg in res if seg is not None], key=lambda x: x.num)
            seg_stat_map = {seg.num: seg for seg in matched_seg_states}
            for i, req_seg in enumerate(matched_req_segments):
                seg_state = seg_stat_map.get(req_seg.num)
                if seg_state is None:
                    log.error(f"ReqSegment not found for num {req_seg.num}", self.__attr)
                    return False
                if req_seg.url != seg_state.url:
                    log.error("URL mismatch", self.__pair_attr(req_seg, seg_state))
                    return await self.__update_to_invalid_live()
                if req_seg.duration != seg_state.duration:
                    log.error("Duration mismatch", self.__pair_attr(req_seg, seg_state))
                    return await self.__update_to_invalid_live()
                if i == len(matched_seg_states) - 1:
                    req_b = await self.__seg_http.get_bytes(url=req_seg.url, retry_limit=self.__req_retry_limit)
                    if len(req_b) != seg_state.size:
                        log.error("Size mismatch", self.__pair_attr(req_seg, seg_state, len(req_b)))
                        return await self.__update_to_invalid_live()
            return True
        except BaseException as ex:
            log.error("Validate segments failed", self.__error_attr(ex))
            return False

    async def validate_segment(self, num: int, success_nums: SegmentNumberSet) -> SegmentInspect:
        try:
            if not await success_nums.get(num):
                return SegmentInspect(ok=True, critical=False, attr=self.__attr)

            seg = await self.__seg_state_service.get(num)
            if seg is None:
                log.error(f"Segment {num} not found", self.__attr)
                return SegmentInspect(ok=False, critical=True, attr=self.__attr)

            if self.__is_invalid_seg(seg):
                await self.__update_to_invalid_live()
                return SegmentInspect(ok=False, critical=True, attr=self.__state_attr(seg))
            else:
                return SegmentInspect(ok=False, critical=False, attr=self.__state_attr(seg))
        except BaseException as ex:
            attr = self.__error_attr(ex)
            log.error("Validate segment failed", attr)
            return SegmentInspect(ok=False, critical=True, attr=attr)

    async def __update_to_invalid_live(self) -> bool:  # only return False
        live = await self.__live_state_service.get(self.__seg_state_service.live_record_id)
        if live is not None:
            live.is_invalid = True
            await self.__live_state_service.set(live, nx=False)
            log.error("LiveState marked as invalid", self.__attr)
        return False

    def __is_invalid_seg(self, seg: SegmentState) -> bool:
        diff = datetime.now() - seg.created_at
        return diff.seconds > self.__invalid_seg_time_diff_threshold_sec

    def __pair_attr(self, req_seg: Segment, seg_state: SegmentState, req_size: int | None = None) -> dict[str, Any]:
        attr = self.__attr.copy()
        attr["req_seg"] = req_seg.to_dict()
        if req_size is not None:
            attr["req_seg"]["size"] = req_size
        attr["seg_state"] = seg_state.to_dict()
        return attr

    def __state_attr(self, seg: SegmentState) -> dict[str, Any]:
        attr = self.__attr.copy()
        attr["seg_state"] = seg.to_dict()
        attr["current_time"] = datetime.now().isoformat()
        return attr
    
    def __error_attr(self, ex: BaseException):
        attr = self.__attr.copy()
        for k, v in error_dict(ex).items():
            attr[k] = v
        return attr
