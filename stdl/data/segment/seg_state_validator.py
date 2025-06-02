from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pyutils import log, error_dict

from .seg_num_set import SegmentNumberSet
from .seg_state_service import SegmentState, SegmentStateService
from ..live import LiveStateService
from ...utils import AsyncHttpClient


class SegmentInspect(BaseModel):
    ok: bool
    critical: bool

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

    async def validate_segments(
        self,
        req_segments: list[SegmentState],
        latest_num: int | None,
        success_nums: SegmentNumberSet,
    ) -> SegmentInspect:
        try:
            log.debug("Validate segments", self.__attr)
            if len(req_segments) == 0:
                log.error("segments is empty", self.__attr)
                return no()

            if latest_num is None:
                return ok()  # init recording

            live = await self.__live_state_service.get_live(self.__seg_state_service.live_record_id, use_master=False)
            if live is None:
                log.error("LiveState not found", self.__attr)
                return no()
            if live.is_invalid:
                log.error("LiveState is invalid", self.__attr)
                return no()

            sorted_req_segments = sorted(req_segments, key=lambda x: x.num)
            matched_nums = await success_nums.range(sorted_req_segments[0].num, sorted_req_segments[-1].num)
            if len(matched_nums) == 0 and self.__is_invalid_num(sorted_req_segments[-1].num, latest_num):
                attr = self.__seg_attr(sorted_req_segments[-1])
                attr["latest_num"] = latest_num
                log.error("Segment number difference is too large", attr)
                return critical()

            matched_seg_states = await self.__seg_state_service.get_batch(matched_nums)
            seg_stat_map = {seg.num: seg for seg in matched_seg_states}

            matched_req_segments = [seg for seg in sorted_req_segments if seg.num in matched_nums]
            for i, req_seg in enumerate(matched_req_segments):
                seg_state = seg_stat_map.get(req_seg.num)
                if seg_state is None:
                    log.error(f"ReqSegment not found for num {req_seg.num}", self.__attr)
                    return no()
                if not self.__validate_segment_pair(req_seg, seg_state):
                    return critical()
                if i == 0:
                    req_b = await self.__seg_http.get_bytes(url=req_seg.url, retry_limit=self.__req_retry_limit)
                    if len(req_b) != seg_state.size:
                        log.error("Size mismatch", self.__pair_attr(req_seg, seg_state, len(req_b)))
                        if seg_state.size is None:
                            return no()
                        else:
                            return critical()
            return ok()
        except BaseException as ex:
            log.error("Validate segments failed", self.__error_attr(ex))
            return no()

    async def validate_segment(
        self,
        seg: SegmentState,
        latest_num: int | None,
        success_nums: SegmentNumberSet,
    ) -> SegmentInspect:
        try:
            if latest_num is None:
                return ok()  # init recording

            if self.__is_invalid_num(seg.num, latest_num):
                attr = self.__seg_attr(seg)
                attr["latest_num"] = latest_num
                log.error("Segment number difference is too large", attr)
                return critical()

            if not await success_nums.contains(seg.num):
                return ok()  # unsuccessful segment

            seg_state = await self.__seg_state_service.get(seg.num)
            if seg_state is None:
                log.error(f"Segment {seg.num} not found", self.__seg_attr(seg))
                return no()

            if not self.__validate_segment_pair(seg, seg_state):
                log.error("Invalid duplicated segment", self.__pair_attr(seg, seg_state))
                return critical()
            else:
                log.debug("Duplicated segment", self.__seg_attr(seg))
                return no()
        except BaseException as ex:
            log.error("Failed to validate segment", self.__error_attr(ex))
            return no()

    def __is_invalid_num(self, num: int, latest_num: int) -> bool:
        return abs(num - latest_num) > self.__invalid_seg_num_diff_threshold

    def __validate_segment_pair(self, req_seg: SegmentState, old_seg: SegmentState) -> bool:
        if req_seg.url != old_seg.url:
            log.error("URL mismatch", self.__pair_attr(req_seg, old_seg))
            return False
        if req_seg.duration != old_seg.duration:
            log.error("Duration mismatch", self.__pair_attr(req_seg, old_seg))
            return False

        diff = datetime.now() - old_seg.created_at
        if diff.seconds > self.__invalid_seg_time_diff_threshold_sec:
            attr = self.__pair_attr(req_seg, old_seg)
            attr["current_time"] = datetime.now().isoformat()
            log.error("created_at mismatch", attr)
            return False

        return True

    def __pair_attr(
        self, req_seg: SegmentState, seg_state: SegmentState, req_size: int | None = None
    ) -> dict[str, Any]:
        attr = self.__attr.copy()
        attr["seg_request"] = req_seg.to_dict()
        if req_size is not None:
            attr["seg_request"]["size"] = req_size
        attr["seg_state"] = seg_state.to_dict()
        return attr

    def __seg_attr(self, seg: SegmentState) -> dict[str, Any]:
        attr = self.__attr.copy()
        attr["seg_state"] = seg.to_dict()
        attr["current_time"] = datetime.now().isoformat()
        return attr

    def __error_attr(self, ex: BaseException):
        attr = self.__attr.copy()
        for k, v in error_dict(ex).items():
            attr[k] = v
        return attr


def ok():
    return SegmentInspect(ok=True, critical=False)


def no():
    return SegmentInspect(ok=False, critical=False)


def critical():
    return SegmentInspect(ok=False, critical=True)
