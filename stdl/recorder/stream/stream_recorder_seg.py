import asyncio
import json
import random
from datetime import datetime

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
from ...data.segment import SegmentNumberSet, SegmentStateService, SegmentStateValidator, SegmentState
from ...file import ObjectWriter
from ...metric import metric
from ...utils import AsyncHttpClient, AsyncCounter

TEST_FLAG = False
# TEST_FLAG = True  # TODO: remove this line after testing

MAP_NUM = -1
INIT_PARALLEL_LIMIT = 1
FIRST_SEG_LOCK_NUM = 0
INTERVAL_MIN_DURATION_SEC = 1
INTERVAL_WAIT_WEIGHT_SEC = 0.2
SEG_TASK_PREFIX = "seg"


class SegmentedStreamRecorder(StreamRecorder):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        redis: Redis,
        redis_data_conf: RedisDataConfig,
        req_conf: RequestConfig,
        incomplete_dir_path: str,
    ):
        super().__init__(live, args, writer, incomplete_dir_path)
        self.m3u8_retry_limit = req_conf.m3u8_retry_limit
        self.seg_parallel_retry_limit = req_conf.seg_parallel_retry_limit
        self.seg_failure_threshold_ratio = req_conf.seg_failure_threshold_ratio

        self.redis = redis
        self.live = live
        self.redis_data_conf = redis_data_conf

        self.idx = 0
        self.done_flag = False

        self.m3u8_duration_hist = metric.create_m3u8_request_duration_histogram()
        self.seg_duration_hist = metric.create_segment_request_duration_histogram()
        self.seg_retry_hist = metric.create_segment_request_retry_histogram()
        self.seg_failure_counter = AsyncCounter()
        self.seg_request_counter = AsyncCounter()
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

        self.retrying_nums = self.__create_num_seg("retrying")
        self.success_nums = self.__create_num_seg("success")
        self.failed_nums = self.__create_num_seg("failed")
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

    async def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        status = self.ctx.to_status(
            fs_name=self.writer.fs_name,
            num=self.idx,
            status=self.status,
        )
        status.stream_url = None
        dct = status.model_dump(mode="json", by_alias=True, exclude_none=True)

        if with_stats:
            if full_stats:
                dct["stats"] = await self.__get_stats(full=True)
            else:
                dct["stats"] = await self.__get_stats()

        return dct

    async def __get_stats(self, full: bool = False) -> dict:
        result = {
            "failed_total": self.seg_failure_counter.get(),
            "segment_request_total": self.seg_request_counter.get(),
            "segment_request_retry_total": self.seg_retry_hist.total_sum,
            "segment_request_retry_avg": round(self.seg_retry_hist.avg(), 3),
            "segment_request_duration_avg": round(self.seg_duration_hist.avg(), 3),
            "m3u8_request_duration_avg": round(self.m3u8_duration_hist.avg(), 3),
        }
        if full:
            result["redis_using_connection_count"] = len(self.redis.connection_pool._in_use_connections)
            result["redis_available_connection_count"] = len(self.redis.connection_pool._available_connections)
        return result

    async def __check_status(self):
        while True:
            print(json.dumps(await self.__get_stats(), indent=4))
            await asyncio.sleep(1)

    async def _record(self):
        self.m3u8_http.set_headers(self.ctx.headers)
        self.seg_http.set_headers(self.ctx.headers)

        # Start recording
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))
        await aos.makedirs(self.ctx.tmp_dir_path, exist_ok=True)

        # if TEST_FLAG:
        #     _ = asyncio.create_task(self.__check_status())

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
        for k, v in (await self.__get_stats()).items():
            result_attr[k] = v
        log.info("Finish Recording", result_attr)
        # if TEST_FLAG:
        #     print(generate_latest().decode("utf-8"))
        self.is_done = True

    async def __interval(self, is_init: bool = False):
        start_time = asyncio.get_event_loop().time()
        await self.__renew()

        # Fetch m3u8
        req_start_time = asyncio.get_event_loop().time()
        try:
            m3u8_text = await self.m3u8_http.get_text(
                url=self.ctx.stream_url,
                headers=self.ctx.headers,
                attr=self.ctx.to_dict(),
                print_error=False,
            )
            await self.m3u8_retry_counter.reset()
            await metric.set_m3u8_request_duration(
                duration=asyncio.get_event_loop().time() - req_start_time,
                platform=self.ctx.live.platform,
                extra=self.m3u8_duration_hist,
            )
        except Exception as ex:
            await self.m3u8_retry_counter.increment()
            await metric.set_m3u8_request_duration(
                duration=asyncio.get_event_loop().time() - req_start_time,
                platform=self.ctx.live.platform,
                extra=self.m3u8_duration_hist,
            )
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
        now = datetime.now()
        for raw_seg in raw_segments:
            url = raw_seg.uri
            if self.ctx.stream_base_url is not None:
                url = "/".join([self.ctx.stream_base_url, raw_seg.uri])
            seg = SegmentState(
                url=url,
                num=raw_seg.num,
                duration=raw_seg.duration,
                size=None,
                parallel_limit=INIT_PARALLEL_LIMIT,
                created_at=now,
                updated_at=now,
            )
            segments.append(seg)

        latest_num = await self.success_nums.get_highest()

        if is_init:
            inspected = await self.seg_state_validator.validate_segments(segments, latest_num, self.success_nums)
            if not inspected.ok:
                if inspected.critical:
                    await self.live_state_service.update_is_invalid(record_id=self.live.id, is_invalid=True)
                log.error("Invalid m3u8", self.ctx.to_dict())
                self.done_flag = True
                return

        if self.status != RecordingStatus.RECORDING:
            self.status = RecordingStatus.RECORDING

        # Process segments
        for new_seg in segments:
            if await self.__is_done_seg(new_seg.num):
                continue
            if await self.retrying_nums.contains(new_seg.num):
                continue
            if await self.seg_state_service.is_locked(seg_num=new_seg.num, lock_num=FIRST_SEG_LOCK_NUM):
                continue
            task_name = self.seg_task_name("req", new_seg.num)
            _ = asyncio.create_task(self.__process_segment(new_seg, latest_num), name=task_name)

        for num in await self.retrying_nums.all():
            if await self.__is_done_seg(num):
                continue
            retrying = await self.seg_state_service.get(num)
            if retrying is None:
                raise ValueError(f"Retrying segment {num} not found in Redis")
            task_name = self.seg_task_name("retry", retrying.num)
            _ = asyncio.create_task(self.__process_segment(retrying, latest_num), name=task_name)

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
        wait_sec = random.uniform(0, INTERVAL_WAIT_WEIGHT_SEC)
        duration = asyncio.get_event_loop().time() - start_time
        metric.set_interval_duration(duration, self.ctx.live.platform)
        if duration < INTERVAL_MIN_DURATION_SEC:
            # if the duration is less than the threshold, wait for the remaining time
            wait_sec += INTERVAL_MIN_DURATION_SEC - duration
        await asyncio.sleep(wait_sec)

    async def __is_done_seg(self, seg_num: int) -> bool:
        return await self.success_nums.contains(seg_num) or await self.failed_nums.contains(seg_num)

    async def __process_segment(self, seg: SegmentState, latest_num: int | None):
        if seg.num == MAP_NUM:
            raise ValueError(f"{MAP_NUM} is not a valid segment number")

        inspected = await self.seg_state_validator.validate_segment(
            seg=seg,
            latest_num=latest_num,
            success_nums=self.success_nums,
        )
        if not inspected.ok:
            if inspected.critical:
                await self.live_state_service.update_is_invalid(record_id=self.live.id, is_invalid=True)
                self.done_flag = True
            return

        if "preloading" in seg.url:
            # this is used to check the logic implemented inside `SoopHLSStreamWriter`.
            # if this log is not printed for a long time, this code will be removed.
            log.debug("Preloading Segment", self.ctx.to_dict())

        lock = await self.seg_state_service.acquire_lock(seg)
        if lock is None:
            # log.debug(f"Failed to acquire segment {seg.num}")
            return
        # log.debug(f"{lock}")

        req_start = asyncio.get_event_loop().time()
        try:
            await self.seg_state_service.set(seg, nx=False)
            await self.seg_request_counter.increment()
            if seg.is_retrying:
                await self.seg_state_service.increment_retry_count(seg.num)

            b = await self.seg_http.get_bytes(url=seg.url, attr=self.ctx.to_dict({"num": seg.num}))
            await metric.set_segment_request_duration(
                duration=asyncio.get_event_loop().time() - req_start,
                platform=self.ctx.live.platform,
                extra=self.seg_duration_hist,
            )

            # await asyncio.sleep(4)
            # if TEST_FLAG:  # TODO: remove (only for test)
            #     if random.random() < 0.7:
            #         raise ValueError("Simulated error")

            if await self.success_nums.contains(seg.num):
                return

            seg_path = path_join(self.ctx.tmp_dir_path, f"{seg.num}.ts")
            async with aiofiles.open(seg_path, "wb") as f:
                await f.write(b)

            seg.size = len(b)

            await self.success_nums.set(seg.num)
            await self.retrying_nums.remove(seg.num)
            await self.seg_state_service.set(seg, nx=False)
            if seg.is_retrying:
                await self.retrying_nums.remove(seg.num)
            await self.failed_nums.remove(seg.num)
            await self.seg_state_service.clear_retry_count(seg.num)
        except Exception as ex:
            await metric.set_segment_request_duration(
                duration=asyncio.get_event_loop().time() - req_start,
                platform=self.ctx.live.platform,
                extra=self.seg_duration_hist,
            )
            if not seg.is_retrying:  # first time failed:
                await self.seg_state_service.update_to_retrying(seg.num, self.seg_parallel_retry_limit)
                await self.retrying_nums.set(seg.num)
            else:  # case of retry:
                retry_count = await self.seg_state_service.get_retry_count(seg.num)
                if retry_count >= (self.seg_parallel_retry_limit * self.seg_failure_threshold_ratio):
                    await self.retrying_nums.remove(seg.num)
                    async with self.success_nums.lock():
                        if not await self.success_nums.contains(seg.num):
                            await self.failed_nums.set(seg.num)
                            await self.seg_failure_counter.increment()
                    await metric.inc_segment_request_failures(
                        platform=self.ctx.live.platform,
                    )
                    await metric.set_segment_request_retry(
                        retry_cnt=retry_count,
                        platform=self.ctx.live.platform,
                        extra=self.seg_retry_hist,
                    )
                    await self.seg_state_service.clear_retry_count(seg.num)
                    log.error("Failed to process segment", self.__error_attr(ex, num=seg.num))
        finally:
            try:
                await self.seg_state_service.release_lock(lock)
            except BaseException as ex:
                log.error("Failed to release segment lock", self.__error_attr(ex, num=seg.num))

    async def __close_recording(self):
        current_task = asyncio.current_task()
        tg_tasks = []
        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            if task.get_name().startswith(f"{SEG_TASK_PREFIX}:{self.live.id}"):
                tg_tasks.append(task)
        await asyncio.gather(*tg_tasks)
        await self.helper.check_tmp_dir(self.ctx)

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
            expire_ms=self.redis_data_conf.seg_expire_sec * 1000,
            lock_expire_ms=self.redis_data_conf.lock_expire_ms,
            lock_wait_timeout_sec=self.redis_data_conf.lock_wait_sec,
            attr=self.ctx.to_dict(),
        )

    async def __renew(self):
        co1 = self.retrying_nums.renew()
        co2 = self.success_nums.renew()
        co3 = self.failed_nums.renew()
        await asyncio.gather(co1, co2, co3)

    def seg_task_name(self, sub_name: str, num: int) -> str:
        return f"{SEG_TASK_PREFIX}:{self.live.id}:{sub_name}:{num}"
