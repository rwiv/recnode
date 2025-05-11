import asyncio
import json
import random

import aiofiles
from aiofiles import os as aos
from pyutils import log, path_join, error_dict
from redis.asyncio import Redis
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from .stream_recorder import StreamRecorder
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_schema import RecordingStatus
from ...config import RequestConfig, RedisDataConfig
from ...data.live import LiveState, LiveStateService
from ...data.segment import SegmentNumberSet, SegmentStateService, Segment, SegmentStateValidator
from ...file import AsyncObjectWriter
from ...metric import MetricManager
from ...utils import AsyncHttpClient, AsyncMap, AsyncSet, AsyncCounter

TEST_FLAG = False
# TEST_FLAG = True  # TODO: remove this line after testing

MAP_NUM = -1
SEG_TASK_PREFIX = "seg"


class SegmentedStreamRecorder(StreamRecorder):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: AsyncObjectWriter,
        redis: Redis,
        redis_data_conf: RedisDataConfig,
        req_conf: RequestConfig,
        metric: MetricManager,
        incomplete_dir_path: str,
    ):
        super().__init__(live, args, writer, metric, incomplete_dir_path)
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2
        self.m3u8_retry_limit = req_conf.m3u8_retry_limit
        self.seg_parallel_retry_limit = req_conf.seg_parallel_retry_limit
        self.seg_failure_threshold_ratio = req_conf.seg_failure_threshold_ratio

        self.redis = redis
        self.live = live
        self.redis_data_conf = redis_data_conf

        self.idx = 0
        self.done_flag = False

        self.processing_nums: AsyncSet[int] = AsyncSet()
        self.retrying_segments: AsyncMap[int, Segment] = AsyncMap()
        self.success_nums: AsyncSet[int] = AsyncSet()
        self.failed_segments: AsyncMap[int, Segment] = AsyncMap()

        self.m3u8_duration_hist = metric.create_m3u8_request_duration_histogram()
        self.seg_duration_hist = metric.create_segment_request_duration_histogram()
        self.seg_retry_hist = metric.create_segment_request_retry_histogram()
        self.seg_request_counter = AsyncCounter()
        self.seg_success_counter = AsyncCounter()
        self.seg_failure_counter = AsyncCounter()
        self.m3u8_retry_counter = AsyncCounter()

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

        self.success_nums_redis = self.__create_num_seg("success")
        self.seg_state_service = SegmentStateService(
            client=redis,
            live_record_id=live.id,
            expire_ms=redis_data_conf.seg_expire_sec * 1000,
            lock_expire_ms=redis_data_conf.lock_expire_ms,
            lock_wait_timeout_sec=redis_data_conf.lock_wait_sec,
            attr=self.ctx.to_dict(),
        )
        self.live_state_service = LiveStateService(client=redis)
        self.seg_state_validator = SegmentStateValidator(
            live_state_service=self.live_state_service,
            seg_state_service=self.seg_state_service,
            seg_http=self.seg_http,
            attr=self.ctx.to_dict(),
        )

    def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        status = self.ctx.to_status(
            fs_name=self.writer.fs_name,
            num=self.idx,
            status=self.status,
        )
        status.stream_url = None
        dct = status.model_dump(mode="json", by_alias=True, exclude_none=True)

        if with_stats:
            if full_stats:
                dct["stats"] = self.__get_stats(with_nums=True)
            else:
                dct["stats"] = self.__get_stats()

        return dct

    def __get_stats(self, with_nums: bool = False) -> dict:
        processing_nums = self.processing_nums.list()
        retrying_nums = self.retrying_segments.keys()

        success_cnt = self.seg_success_counter.get()
        failed_cnt = self.seg_failure_counter.get()
        result = {
            "processing_total": len(processing_nums),
            "retrying_total": len(retrying_nums),
            "success_total": success_cnt,
            "failed_total": failed_cnt,
            "done_total": success_cnt + failed_cnt,
            "segment_request_total": self.seg_request_counter.get(),
            "segment_request_retry_total": self.seg_retry_hist.total_sum,
            "segment_request_retry_avg": round(self.seg_retry_hist.avg(), 3),
            "segment_request_duration_avg": round(self.seg_duration_hist.avg(), 3),
            "m3u8_request_duration_avg": round(self.m3u8_duration_hist.avg(), 3),
        }
        if with_nums:
            result["processing_nums"] = processing_nums
            result["retrying_nums"] = retrying_nums
            result["failed_nums"] = self.failed_segments.keys()
            result["success_nums"] = self.success_nums.list()
        return result

    def record(self):
        self.recording_task = asyncio.create_task(self.__record(), name=f"recording:{self.live.id}")

    async def __record(self):
        self.m3u8_http.set_headers(self.ctx.headers)
        self.seg_http.set_headers(self.ctx.headers)

        # Start recording
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))

        try:
            await self.__interval(is_init=True)

            while True:
                if self.done_flag:
                    self.status = RecordingStatus.DONE
                    log.debug("Finish Stream", self.ctx.to_dict())
                    break
                if self.state.abort_flag:
                    self.status = RecordingStatus.DONE
                    log.debug("Abort Stream", self.ctx.to_dict())
                    break

                await self.__interval()
        except Exception as e:
            log.error("Error during recording", self.ctx.to_err(e))
            self.status = RecordingStatus.FAILED

        await self.__close_recording()
        result_attr = self.ctx.to_dict()
        for k, v in self.__get_stats().items():
            result_attr[k] = v
        log.info("Finish Recording", result_attr)
        # if TEST_FLAG:
        #     print(generate_latest().decode("utf-8"))
        self.is_done = True

    async def __interval(self, is_init: bool = False):
        if TEST_FLAG:
            print(json.dumps(self.__get_stats(), indent=4))

        await self.success_nums_redis.renew()
        await aos.makedirs(self.ctx.tmp_dir_path, exist_ok=True)

        # Fetch m3u8
        try:
            start_time = asyncio.get_event_loop().time()
            m3u8_text = await self.m3u8_http.get_text(
                url=self.ctx.stream_url,
                headers=self.ctx.headers,
                attr=self.ctx.to_dict(),
                print_error=False,
            )
            await self.m3u8_retry_counter.reset()
            await self.metric.set_m3u8_request_duration(
                duration=asyncio.get_event_loop().time() - start_time,
                platform=self.ctx.live.platform,
                extra=self.m3u8_duration_hist,
            )
        except Exception as ex:
            await self.m3u8_retry_counter.increment()
            live_info = await self.fetcher.fetch_live_info(self.ctx.live_url)
            if live_info is None:
                self.done_flag = True
                return
            else:
                log.error("Failed to get playlist", self.ctx.to_err(ex))
                return
        finally:
            if self.m3u8_retry_counter.get() >= self.m3u8_retry_limit:
                log.error("Max retry limit reached for m3u8", self.ctx.to_dict())
                self.done_flag = True
                return

        playlist: M3U8 = M3U8Parser().parse(m3u8_text)
        if playlist.is_master:
            raise ValueError("Expected a media playlist, got a master playlist")
        raw_segments: list[HLSSegment] = playlist.segments
        if len(raw_segments) == 0:
            raise ValueError("No segments found in the playlist")

        # If the first segment has a map, download it
        map_seg = raw_segments[0].map
        if is_init and map_seg is not None:
            map_url = map_seg.uri
            if self.ctx.stream_base_url is not None:
                map_url = "/".join([self.ctx.stream_base_url, map_seg.uri])
            b = await self.seg_http.get_bytes(map_url, attr=self.ctx.to_dict())
            async with aiofiles.open(path_join(self.ctx.tmp_dir_path, f"{MAP_NUM}.ts"), "wb") as f:
                await f.write(b)

        segments = []
        for raw_seg in raw_segments:
            url = raw_seg.uri
            if self.ctx.stream_base_url is not None:
                url = "/".join([self.ctx.stream_base_url, raw_seg.uri])
            seg = Segment(num=raw_seg.num, url=url, duration=raw_seg.duration, limit=self.seg_parallel_retry_limit)
            segments.append(seg)

        ok = await self.seg_state_validator.validate_segments(segments, self.success_nums_redis)
        if not ok:
            log.error("Invalid m3u8", self.ctx.to_dict())
            self.done_flag = True
            return

        if self.status != RecordingStatus.RECORDING:
            self.status = RecordingStatus.RECORDING

        # Process segments
        for seg in segments:
            if self.__is_done_seg(seg.num) or self.processing_nums.contains(seg.num):
                continue
            _ = asyncio.create_task(self.__process_segment(seg), name=self.seg_tname("req", seg.num))

        for _, failed in self.retrying_segments.items():
            if self.__is_done_seg(failed.num):
                continue
            _ = asyncio.create_task(self.__process_segment(failed), name=self.seg_tname("retry", failed.num))

        # Upload segments tar
        target_segments = await self.helper.check_segments(self.ctx)
        if target_segments is not None and len(target_segments) > 0:
            tar_path = await asyncio.to_thread(self.helper.archive_files, target_segments, self.ctx.tmp_dir_path)
            self.helper.start_write_segment_task(tar_path, self.ctx)

        self.idx = raw_segments[-1].num

        if playlist.is_endlist:
            self.done_flag = True
            return

        # to prevent segment requests from being concentrated on a specific node
        await asyncio.sleep(random.uniform(self.min_delay_sec, self.max_delay_sec))

    def __is_done_seg(self, seg_num: int) -> bool:
        return self.success_nums.contains(seg_num) or self.failed_segments.contains(seg_num)

    async def __process_segment(self, seg: Segment):
        if seg.num == MAP_NUM:
            raise ValueError(f"{MAP_NUM} is not a valid segment number")

        # Check if have permission to segment
        if not seg.is_failed:
            # TODO: Implement segment lock acquisition logic
            await self.processing_nums.add(seg.num)
        else:
            if not await seg.acquire():
                # log.debug("Failed to acquire segment")
                return

        inspected = await self.seg_state_validator.validate_segment(seg.num, self.success_nums_redis)
        if not inspected.ok:
            log.warn("Detect duplicated segment", inspected.attr)
            if inspected.critical:
                log.error("Detect invalid segment", inspected.attr)
                self.done_flag = True
            return

        if "preloading" in seg.url:
            # this is used to check the logic implemented inside `SoopHLSStreamWriter`.
            # if this log is not printed for a long time, this code will be removed.
            log.debug("Preloading Segment", self.ctx.to_dict())

        req_start = asyncio.get_event_loop().time()
        try:
            await self.seg_request_counter.increment()
            if seg.is_failed:
                await seg.increment_retry_count()

            b = await self.seg_http.get_bytes(url=seg.url, attr=self.ctx.to_dict({"num": seg.num}))
            await self.metric.set_segment_request_duration(
                duration=asyncio.get_event_loop().time() - req_start,
                platform=self.ctx.live.platform,
                extra=self.seg_duration_hist,
            )

            # await asyncio.sleep(2)
            # if TEST_FLAG and not seg.is_failed:  # TODO: remove (only for test)
            #     if random.random() < 0.8:
            #         raise ValueError("Simulated error")

            if self.success_nums.contains(seg.num):
                return

            seg_path = path_join(self.ctx.tmp_dir_path, f"{seg.num}.ts")
            async with aiofiles.open(seg_path, "wb") as f:
                await f.write(b)

            await self.success_nums.add(seg.num)
            await self.success_nums_redis.set(seg.num)
            await self.seg_state_service.set_nx(seg.to_new_state(len(b)))
            await self.processing_nums.remove(seg.num)
            if seg.is_failed:
                await self.retrying_segments.remove(seg.num)
            await self.failed_segments.remove(seg.num)

            await self.seg_success_counter.increment()
            await self.metric.set_segment_request_retry(
                retry_cnt=seg.retry_count,
                platform=self.ctx.live.platform,
                extra=self.seg_retry_hist,
            )
        except Exception as ex:
            await self.metric.set_segment_request_duration(
                duration=asyncio.get_event_loop().time() - req_start,
                platform=self.ctx.live.platform,
                extra=self.seg_duration_hist,
            )
            if not seg.is_failed:  # first time failed:
                seg.is_failed = True
                await self.retrying_segments.set(seg.num, seg)
            else:  # case of retry:
                if seg.retry_count >= (self.seg_parallel_retry_limit * self.seg_failure_threshold_ratio):
                    await self.processing_nums.remove(seg.num)
                    await self.retrying_segments.remove(seg.num)
                    async with self.success_nums.lock:
                        if not self.success_nums.contains(seg.num):
                            await self.failed_segments.set(seg.num, seg)
                    await self.metric.inc_segment_request_failures(
                        platform=self.ctx.live.platform,
                        extra=self.seg_failure_counter,
                    )
                    await self.metric.set_segment_request_retry(
                        retry_cnt=seg.retry_count,
                        platform=self.ctx.live.platform,
                        extra=self.seg_retry_hist,
                    )
                    log.error("Failed to process segment", self.__error_attr(ex, num=seg.num))
                await seg.release()

    async def __close_recording(self):
        await self.helper.check_tmp_dir(self.ctx)
        current_task = asyncio.current_task()
        tg_tasks = []
        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            if task.get_name().startswith(f"seg:{self.live.id}"):
                tg_tasks.append(task)
        await asyncio.gather(*tg_tasks)

    def __error_attr(self, ex: BaseException, num: int | None = None):
        attr = self.ctx.to_dict()
        for k, v in error_dict(ex).items():
            attr[k] = v
        if num is not None:
            attr["num"] = num
        return attr

    def __create_num_seg(self, suffix: str):
        return SegmentNumberSet(
            client=self.redis,
            live_record_id=self.live.id,
            key_suffix=suffix,
            expire_ms=self.redis_data_conf.live_expire_sec * 1000,
            lock_expire_ms=self.redis_data_conf.lock_expire_ms,
            lock_wait_timeout_sec=self.redis_data_conf.lock_wait_sec,
        )

    def seg_tname(self, sub_name: str, num: int) -> str:
        return f"{SEG_TASK_PREFIX}:{self.live.id}:{sub_name}:{num}"
