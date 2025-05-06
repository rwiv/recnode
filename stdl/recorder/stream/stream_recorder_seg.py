import asyncio
import json
import random
import time

import aiofiles
from pyutils import log, path_join, error_dict
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from .stream_helper import StreamHelper
from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_schema import RecordingState, RecordingStatus
from ...common.env import RequestConfig
from ...data.live import LiveState
from ...fetcher import PlatformFetcher
from ...file import ObjectWriter
from ...utils import AsyncHttpClient, HttpRequestError, AsyncMap, AsyncSet, AsyncCounter

TEST_FLAG = False
# TEST_FLAG = True  # TODO: remove this line after testing


class Segment:
    def __init__(self, num: int, url: str, limit: int):
        self.num = num
        self.url = url
        self.limit = limit

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


class SegmentedStreamRecorder:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
        req_conf: RequestConfig,
    ):
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2
        self.m3u8_retry_limit = req_conf.m3u8_retry_limit
        self.seg_parallel_retry_limit = req_conf.seg_parallel_retry_limit
        self.seg_failure_threshold_ratio = req_conf.seg_failure_threshold_ratio

        self.idx = 0
        self.done_flag = False
        self.state = RecordingState()
        self.status_type: RecordingStatus = RecordingStatus.WAIT
        self.processing_nums: AsyncSet[int] = AsyncSet()
        self.retrying_segments: AsyncMap[int, Segment] = AsyncMap()
        self.success_nums: AsyncSet[int] = AsyncSet()
        self.failed_segments: AsyncMap[int, Segment] = AsyncMap()
        self.request_counts: AsyncMap[int, AsyncCounter] = AsyncMap()
        self.req_durations: list[float] = []
        self.seg_retry_counter = AsyncCounter()
        self.m3u8_retry_counter = AsyncCounter()
        self.ctx: RequestContext | None = None

        self.m3u8_http = AsyncHttpClient(
            timeout_sec=req_conf.m3u8_timeout_sec,
            retry_limit=0,
            retry_delay_sec=0,
            print_error=False,
        )
        self.seg_http = AsyncHttpClient(
            timeout_sec=req_conf.seg_timeout_sec,
            retry_limit=0,
            retry_delay_sec=0,
            print_error=False,
        )
        self.fetcher = PlatformFetcher()
        self.writer = writer
        self.helper = StreamHelper(
            args, self.state, writer, self.fetcher, incomplete_dir_path=incomplete_dir_path
        )

    def check_tmp_dir(self):
        assert self.ctx is not None
        self.helper.check_tmp_dir(self.ctx)

    def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        assert self.ctx is not None
        status = self.ctx.to_status(
            fs_name=self.writer.fs_name,
            num=self.idx,
            status=self.status_type,
        )
        status.stream_url = None
        dct = status.model_dump(mode="json", by_alias=True, exclude_none=True)

        if with_stats:
            if full_stats:
                dct["stats"] = self.get_stats(with_nums=True)
            else:
                dct["stats"] = self.get_stats()

        return dct

    def get_stats(self, with_nums: bool = False) -> dict:
        processing_nums = self.processing_nums.list()
        success_nums = self.success_nums.list()
        retry_nums = self.retrying_segments.keys()
        failed_nums = self.failed_segments.keys()
        req_counts = [counter.get() for counter in self.request_counts.map.values()]
        req_duration_size = len(self.req_durations)

        success_cnt = len(success_nums)
        failed_cnt = len(failed_nums)
        done_cnt = success_cnt + failed_cnt
        result = {
            "processing_total": len(processing_nums),
            "retrying_total": len(retry_nums),
            "success_total": success_cnt,
            "failed_total": failed_cnt,
            "done_total": done_cnt,
            "retry_total": self.seg_retry_counter.get(),
            "seg_request_total": sum(req_counts),
            "seg_request_try_min": min(req_counts) if len(req_counts) > 0 else 0,
            "seg_request_try_max": max(req_counts) if len(req_counts) > 0 else 0,
            "seg_request_try_avg": round(sum(req_counts) / len(req_counts), 3) if len(req_counts) > 0 else 0,
            "seg_request_duration_min": round(min(self.req_durations), 3) if req_duration_size > 0 else 0,
            "seg_request_duration_max": round(max(self.req_durations), 3) if req_duration_size > 0 else 0,
            "seg_request_duration_avg": (
                round(sum(self.req_durations) / req_duration_size, 3) if req_duration_size > 0 else 0
            ),
        }
        if with_nums:
            result["processing_nums"] = processing_nums
            result["retrying_nums"] = retry_nums
            result["failed_nums"] = failed_nums
            result["success_nums"] = success_nums
        return result

    async def record(self, state: LiveState):
        self.ctx = await self.helper.get_ctx(state)
        self.m3u8_http.set_headers(self.ctx.headers)
        self.seg_http.set_headers(self.ctx.headers)

        # Start recording
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))

        try:
            await self.__interval(is_init=True)
            self.status_type = RecordingStatus.RECORDING

            while True:
                if self.done_flag:
                    self.status_type = RecordingStatus.DONE
                    log.debug("Finish Stream", self.ctx.to_dict())
                    break
                if self.state.abort_flag:
                    self.status_type = RecordingStatus.DONE
                    log.debug("Abort Stream", self.ctx.to_dict())
                    break

                await self.__interval()
        except Exception as e:
            log.error("Error during recording", self.ctx.to_err(e))
            self.status_type = RecordingStatus.FAILED

        self.__close_recording()
        result_attr = self.ctx.to_dict()
        for k, v in self.get_stats().items():
            result_attr[k] = v
        log.info("Finish Recording", result_attr)
        return self.ctx.live

    async def __interval(self, is_init: bool = False):
        assert self.ctx is not None

        if TEST_FLAG:
            print(json.dumps(self.get_stats(), indent=4))

        # Fetch m3u8
        try:
            m3u8_text = await self.m3u8_http.get_text(
                self.ctx.stream_url, self.ctx.headers, attr=self.ctx.to_dict(), print_error=False
            )
            await self.m3u8_retry_counter.reset()
        except HttpRequestError as e:
            await self.m3u8_retry_counter.increment()
            live_info = await self.fetcher.fetch_live_info(self.ctx.live_url)
            if live_info is None:
                self.done_flag = True
                return
            else:
                log.error("Failed to get playlist", self.ctx.to_err(e))
                return
        except Exception as e:
            await self.m3u8_retry_counter.increment()
            log.error("Failed to get playlist", self.ctx.to_err(e))
            return
        finally:
            if self.m3u8_retry_counter.get() >= self.m3u8_retry_limit:
                log.error("Max retry limit reached for m3u8", self.ctx.to_dict())
                self.done_flag = True
                return

        playlist: M3U8 = M3U8Parser().parse(m3u8_text)
        if playlist.is_master:
            raise ValueError("Expected a media playlist, got a master playlist")
        segments: list[HLSSegment] = playlist.segments
        if len(segments) == 0:
            raise ValueError("No segments found in the playlist")

        # If the first segment has a map, download it
        map_seg = segments[0].map
        if is_init and map_seg is not None:
            map_url = map_seg.uri
            if self.ctx.stream_base_url is not None:
                map_url = "/".join([self.ctx.stream_base_url, map_seg.uri])
            b = await self.seg_http.get_bytes(map_url, headers=self.ctx.headers, attr=self.ctx.to_dict())
            async with aiofiles.open(path_join(self.ctx.tmp_dir_path, "-1.ts"), "wb") as f:
                await f.write(b)

        # Process segments
        for raw_seg in segments:
            if self.is_done_seg(raw_seg.num) or self.processing_nums.contains(raw_seg.num):
                continue

            url = raw_seg.uri
            if self.ctx.stream_base_url is not None:
                url = "/".join([self.ctx.stream_base_url, raw_seg.uri])
            seg = Segment(num=raw_seg.num, url=url, limit=self.seg_parallel_retry_limit)
            _ = asyncio.create_task(self.__process_segment(seg))

        for _, failed in self.retrying_segments.items():
            if self.is_done_seg(failed.num):
                continue
            _ = asyncio.create_task(self.__process_segment(failed))

        # Upload segments tar
        target_segments = await self.helper.check_segments(self.ctx)
        if target_segments is not None and len(target_segments) > 0:
            tar_path = self.helper.archive(target_segments, self.ctx.tmp_dir_path)
            self.helper.write_segment_thread(tar_path, self.ctx)

        self.idx = segments[-1].num

        if playlist.is_endlist:
            self.done_flag = True
            return

        # to prevent segment requests from being concentrated on a specific node
        await asyncio.sleep(random.uniform(self.min_delay_sec, self.max_delay_sec))

    def is_done_seg(self, seg_num: int) -> bool:
        return self.success_nums.contains(seg_num) or self.failed_segments.contains(seg_num)

    async def __process_segment(self, seg: Segment):
        assert self.ctx is not None

        # Check if have permission to segment
        if not seg.is_failed:
            # TODO: Implement segment lock acquisition logic
            await self.processing_nums.add(seg.num)
        else:
            if not await seg.acquire():
                # log.debug("Failed to acquire segment")
                return

        # Update request count
        req_counter = await self.request_counts.get(seg.num)
        if req_counter is None:
            req_counter = AsyncCounter()
            await self.request_counts.set(seg.num, req_counter)
        await req_counter.increment()

        if seg.is_failed:
            await self.seg_retry_counter.increment()

        if "preloading" in seg.url:
            # this is used to check the logic implemented inside `SoopHLSStreamWriter`.
            # if this log is not printed for a long time, this code will be removed.
            log.debug("Preloading Segment", self.ctx.to_dict())

        req_start = time.time()
        try:
            # if TEST_FLAG:  # TODO: remove (only for test)
            #     await asyncio.sleep(4)
            #     if random.random() < 0.2:
            #         raise ValueError("Simulated error")

            b = await self.seg_http.get_bytes(
                seg.url, headers=self.ctx.headers, attr=self.ctx.to_dict({"num": seg.num})
            )
            self.req_durations.append(time.time() - req_start)
            seg_path = path_join(self.ctx.tmp_dir_path, f"{seg.num}.ts")
            async with aiofiles.open(seg_path, "wb") as f:
                await f.write(b)

            await self.processing_nums.remove(seg.num)
            if seg.is_failed:
                await self.retrying_segments.remove(seg.num)
            await self.success_nums.add(seg.num)
            await self.failed_segments.remove(seg.num)
        except BaseException as ex:
            self.req_durations.append(time.time() - req_start)
            if not seg.is_failed:  # first time failed:
                seg.is_failed = True
                await self.retrying_segments.set(seg.num, seg)
            else:  # case of retry:
                if req_counter.get() >= (self.seg_parallel_retry_limit * self.seg_failure_threshold_ratio):
                    await self.processing_nums.remove(seg.num)
                    await self.retrying_segments.remove(seg.num)
                    async with self.success_nums.lock:
                        if not self.success_nums.contains(seg.num):
                            await self.failed_segments.set(seg.num, seg)
                    log.error("Failed to process segment", self.__error_attr(ex, num=seg.num))
                await seg.release()

    def __close_recording(self):
        assert self.ctx is not None
        self.helper.check_tmp_dir(self.ctx)

    def __error_attr(self, ex: BaseException, num: int | None = None):
        assert self.ctx is not None
        attr = self.ctx.to_dict()
        for k, v in error_dict(ex).items():
            attr[k] = v
        if num is not None:
            attr["num"] = num
        return attr
