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
INTERVAL_MIN_TIME_SEC = 1
INTERVAL_WAIT_WEIGHT_SEC = 0.2
SEG_TASK_PREFIX = "seg"


class SegmentedStreamRecorder(StreamRecorder):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        redis_master: Redis,
        redis_replica: Redis,
        redis_data_conf: RedisDataConfig,
        req_conf: RequestConfig,
        incomplete_dir_path: str,
    ):
        super().__init__(live, args, writer, incomplete_dir_path)
        self.__m3u8_retry_limit = req_conf.m3u8_retry_limit
        self.__seg_parallel_retry_limit = req_conf.seg_parallel_retry_limit
        self.__seg_failure_threshold_ratio = req_conf.seg_failure_threshold_ratio

        self.__redis_master = redis_master
        self.__redis_replica = redis_replica
        self.__redis_data_conf = redis_data_conf

        self.__idx = 0
        self.__done_flag = False

        self.__m3u8_duration_hist = metric.create_m3u8_request_duration_histogram()
        self.__seg_duration_hist = metric.create_segment_request_duration_histogram()
        self.__seg_retry_hist = metric.create_segment_request_retry_histogram()
        self.__seg_failure_counter = AsyncCounter()
        self.__seg_request_counter = AsyncCounter()
        self.__m3u8_retry_counter = AsyncCounter()
        self.__m3u8_retry_counter_total = AsyncCounter()

        self.__m3u8_http = AsyncHttpClient(
            timeout_sec=req_conf.m3u8_timeout_sec,
            retry_limit=0,
            retry_delay_sec=0,
            print_error=False,
        )
        self.__seg_http = AsyncHttpClient(
            timeout_sec=req_conf.seg_timeout_sec,
            retry_limit=0,
            retry_delay_sec=0,
            print_error=False,
        )

        self.__retrying_nums = self.__create_num_seg("retrying")
        self.__success_nums = self.__create_num_seg("success")
        self.__failed_nums = self.__create_num_seg("failed")
        self.__seg_service = SegmentStateService(
            master=redis_master,
            replica=redis_replica,
            live_record_id=live.id,
            expire_ms=redis_data_conf.seg_expire_sec * 1000,
            lock_expire_ms=redis_data_conf.lock_expire_ms,
            lock_wait_timeout_sec=redis_data_conf.lock_wait_sec,
            retry_parallel_retry_limit=self.__seg_parallel_retry_limit,
            attr=self.ctx.to_dict(),
        )
        self.__live_service = LiveStateService(master=redis_master, replica=redis_replica)
        self.__seg_validator = SegmentStateValidator(
            live_service=self.__live_service,
            seg_service=self.__seg_service,
            seg_http=self.__seg_http,
            attr=self.ctx.to_dict(),
        )

    async def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        status = self.ctx.to_status(fs_name=self._writer.fs_name, num=self.__idx, status=self._status)
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
            "failed_total": self.__seg_failure_counter.get(),
            "segment_request_total": self.__seg_request_counter.get(),
            "segment_request_retries_total": self.__seg_retry_hist.total_sum,
            "segment_request_retries_avg": round(self.__seg_retry_hist.avg(), 3),
            "segment_request_duration_avg": round(self.__seg_duration_hist.avg(), 3),
            "m3u8_request_duration_avg": round(self.__m3u8_duration_hist.avg(), 3),
            "m3u8_request_retries_total": self.__m3u8_retry_counter_total.get(),
        }
        if full:
            result["redis_using_connection_count"] = len(self.__redis_master.connection_pool._in_use_connections)
            result["redis_available_connection_count"] = len(self.__redis_master.connection_pool._available_connections)
        return result

    async def __check_status(self):
        while True:
            print(json.dumps(await self.__get_stats(), indent=4))
            await asyncio.sleep(1)

    async def _record(self):
        self.__m3u8_http.set_headers(self.ctx.headers)
        self.__seg_http.set_headers(self.ctx.headers)

        # Start recording
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))
        await aos.makedirs(self.ctx.tmp_dir_path, exist_ok=True)

        # if TEST_FLAG:
        #     _ = asyncio.create_task(self.__check_status())

        try:
            await self.__interval(is_init=True)

            while True:
                if self.__done_flag:
                    self._status = RecordingStatus.DONE
                    log.debug("Finish Stream", self.ctx.to_dict())
                    break
                if self._state.abort_flag:
                    self._status = RecordingStatus.DONE
                    log.debug("Abort Stream", self.ctx.to_dict())
                    break

                await self.__interval()
        except Exception as e:
            log.error("Error during recording", self.ctx.to_err(e))
            self._status = RecordingStatus.FAILED

        await self.__close_recording()
        result_attr = self.ctx.to_dict()
        for k, v in (await self.__get_stats()).items():
            result_attr[k] = v
        result_attr["failed_total"] = await self.__failed_nums.size(use_master=False)
        log.info("Finish Recording", result_attr)
        self.is_done = True

    async def __interval(self, is_init: bool = False):
        start_time = asyncio.get_event_loop().time()
        await self.__renew()

        # Fetch m3u8
        req_start_time = asyncio.get_event_loop().time()
        try:
            m3u8_text = await self.__m3u8_http.get_text(
                url=self.ctx.stream_url,
                headers=self.ctx.headers,
                attr=self.ctx.to_dict(),
                print_error=False,
            )
            await self.__m3u8_retry_counter.reset()
            await metric.set_m3u8_request_duration(
                duration=asyncio.get_event_loop().time() - req_start_time,
                platform=self.ctx.live.platform,
                extra=self.__m3u8_duration_hist,
            )
        except Exception as ex:
            await self.__m3u8_retry_counter.increment()
            await metric.inc_m3u8_request_retry(platform=self.ctx.live.platform, extra=self.__m3u8_retry_counter_total)
            duration = asyncio.get_event_loop().time() - req_start_time
            await metric.set_m3u8_request_duration(duration, self.ctx.live.platform, self.__m3u8_duration_hist)
            live_info = await self._fetcher.fetch_live_info(self.ctx.live_url)
            if live_info is None:
                self.__done_flag = True
                return
            # log.debug("Failed to get playlist", self.ctx.to_err(ex))
            if self.__m3u8_retry_counter.get() >= self.__m3u8_retry_limit:
                log.error("Max retry limit reached for m3u8", self.ctx.to_err(ex))
                self.__done_flag = True
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
            b = await self.__seg_http.get_bytes(map_url, attr=self.ctx.to_dict())
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

        latest_num = await self.__success_nums.get_highest(use_master=False)

        if is_init:
            inspected = await self.__seg_validator.validate_segments(segments, latest_num, self.__success_nums)
            if not inspected.ok:
                if inspected.critical:
                    await self.__live_service.update_is_invalid(record_id=self._live.id, is_invalid=True)
                log.error("Invalid m3u8", self.ctx.to_dict())
                self.__done_flag = True
                return

        if self._status != RecordingStatus.RECORDING:
            self._status = RecordingStatus.RECORDING

        # Process segments
        for new_seg in segments:
            task_name = self.seg_task_name("req", new_seg.num)
            _ = asyncio.create_task(self.__process_segment(new_seg, latest_num), name=task_name)

        # If the first segment is not MAP_NUM, it means it is a valid segment
        retry_nums = await self.__retrying_nums.all(use_master=False)
        for retrying in await self.__seg_service.get_batch(retry_nums, use_master=False):
            task_name = self.seg_task_name("retry", retrying.num)
            _ = asyncio.create_task(self.__process_segment(retrying, latest_num), name=task_name)

        # Upload segments tar
        target_segments = await self._helper.check_segments(self.ctx)
        if target_segments is not None and len(target_segments) > 0:
            tar_path = await asyncio.to_thread(self._helper.archive_files, target_segments, self.ctx.tmp_dir_path)
            self._helper.start_write_segment_task(tar_path, self.ctx)

        self.__idx = raw_segments[-1].num

        if playlist.is_endlist:
            self.__done_flag = True
            return

        duration = asyncio.get_event_loop().time() - start_time
        wait_sec = 0
        if duration < INTERVAL_MIN_TIME_SEC:
            wait_sec += INTERVAL_MIN_TIME_SEC - duration
        # to prevent segment requests from being concentrated on a specific node
        wait_sec += random.uniform(0, INTERVAL_WAIT_WEIGHT_SEC)
        await asyncio.sleep(wait_sec)
        metric.set_interval_duration(asyncio.get_event_loop().time() - start_time, self.ctx.live.platform)

    async def __process_segment(self, seg: SegmentState, latest_num: int | None):
        if seg.num == MAP_NUM:
            raise ValueError(f"{MAP_NUM} is not a valid segment number")

        coroutines = [
            self.__success_nums.contains(seg.num, use_master=False),
            self.__failed_nums.contains(seg.num, use_master=False),
        ]
        if not seg.is_retrying:
            coroutines.append(self.__retrying_nums.contains(seg.num, use_master=False))
            coroutines.append(self.__seg_service.is_locked(seg_num=seg.num, lock_num=FIRST_SEG_LOCK_NUM, use_master=False))
        if any(await asyncio.gather(*coroutines)):
            return

        inspected = await self.__seg_validator.validate_segment(seg, latest_num, self.__success_nums)
        if not inspected.ok:
            if inspected.critical:
                await self.__live_service.update_is_invalid(record_id=self._live.id, is_invalid=True)
                self.__done_flag = True
            return

        lock = await self.__seg_service.acquire_lock(seg)  # master +1
        if lock is None:
            # log.debug(f"Failed to acquire segment {seg.num}")
            return
        # log.debug(f"{lock}")

        req_start = asyncio.get_event_loop().time()
        try:
            if not seg.is_retrying:
                ok = await self.__seg_service.set_seg(seg, nx=True)  # master +1
                if not ok:
                    log.error("Segment already exists", self.ctx.to_dict({"num": seg.num}))
                    return
            else:
                await self.__seg_service.increment_retry_count(seg.num)  # master +1~2
            await self.__seg_request_counter.increment()

            b = await self.__seg_http.get_bytes(url=seg.url, attr=self.ctx.to_dict({"num": seg.num}))
            duration = asyncio.get_event_loop().time() - req_start
            await metric.set_segment_request_duration(duration, self.ctx.live.platform, self.__seg_duration_hist)

            # await asyncio.sleep(4)
            # if TEST_FLAG:  # TODO: remove (only for test)
            #     if random.random() < 0.7:
            #         raise ValueError("Simulated error")

            if await self.__success_nums.contains(seg.num, use_master=False):
                return

            seg_path = path_join(self.ctx.tmp_dir_path, f"{seg.num}.ts")
            async with aiofiles.open(seg_path, "wb") as f:
                await f.write(b)
            try:
                await self.__on_segment_request_success(seg, size=len(b))
            except Exception as ex:
                log.error("Failed to success process", self.__error_attr(ex, num=seg.num))
        except Exception as ex:
            try:
                await self.__on_segment_request_failure(seg, req_start, ex)
            except Exception as ex:
                log.error("Failed to failure process", self.__error_attr(ex, num=seg.num))
        finally:
            try:
                await self.__seg_service.release_lock(lock)  # master +1
            except BaseException as ex:
                log.error("Failed to release segment lock", self.__error_attr(ex, num=seg.num))

    async def __on_segment_request_success(self, seg: SegmentState, size: int):
        if seg.is_retrying:
            retry_count = await self.__seg_service.get_retry_count(seg.num, use_master=False)
            await metric.set_segment_request_retry(retry_count, self.ctx.live.platform, self.__seg_retry_hist)
        async with self.__success_nums.lock():  # master +2
            await self.__success_nums.set_num(seg.num)  # master +1
        await asyncio.gather(
            self.__seg_service.update_to_success(state=seg, size=size),  # master +1
            self.__seg_service.clear_retry_count(seg.num),  # master +0~1
            self.__retrying_nums.remove(seg.num),  # master +0~1
            self.__failed_nums.remove(seg.num),  # master +0~1
        )

    async def __on_segment_request_failure(self, seg: SegmentState, req_start: float, ex: BaseException):
        duration = asyncio.get_event_loop().time() - req_start
        await metric.set_segment_request_duration(duration, self.ctx.live.platform, self.__seg_duration_hist)

        if not seg.is_retrying:  # failed to first request:
            async with self.__success_nums.lock():  # master +2
                if not await self.__success_nums.contains(seg.num, use_master=True):  # master +1
                    await asyncio.gather(
                        self.__seg_service.update_to_retrying(seg),  # master +1
                        self.__retrying_nums.set_num(seg.num),  # master +1
                    )
        else:  # failed to retry request:
            retry_count = await self.__seg_service.get_retry_count(seg.num, use_master=False)
            if retry_count >= (self.__seg_parallel_retry_limit * self.__seg_failure_threshold_ratio):
                async with self.__success_nums.lock():  # master +2
                    if not await self.__success_nums.contains(seg.num, use_master=True):  # master +1
                        await asyncio.gather(
                            self.__failed_nums.set_num(seg.num),  # master +1
                            metric.inc_segment_request_failures(self.ctx.live.platform, self.__seg_failure_counter),
                        )
                        log.error("Failed to process segment", self.__error_attr(ex, num=seg.num))
                await asyncio.gather(
                    self.__retrying_nums.remove(seg.num),  # master +1
                    self.__seg_service.clear_retry_count(seg.num),  # master +1
                    metric.set_segment_request_retry(retry_count, self.ctx.live.platform, self.__seg_retry_hist),
                )

    async def __close_recording(self):
        current_task = asyncio.current_task()
        tg_tasks = []
        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            if task.get_name().startswith(f"{SEG_TASK_PREFIX}:{self._live.id}"):
                tg_tasks.append(task)
        await asyncio.gather(*tg_tasks)
        await self._helper.check_tmp_dir(self.ctx)

    def __error_attr(self, ex: BaseException, num: int | None = None):
        attr = self.ctx.to_dict()
        for k, v in error_dict(ex).items():
            attr[k] = v
        if num is not None:
            attr["num"] = num
        return attr

    def __create_num_seg(self, suffix: str):
        return SegmentNumberSet(
            master=self.__redis_master,
            replica=self.__redis_replica,
            live_record_id=self._live.id,
            key_suffix=suffix,
            expire_ms=self.__redis_data_conf.seg_expire_sec * 1000,
            lock_expire_ms=self.__redis_data_conf.lock_expire_ms,
            lock_wait_timeout_sec=self.__redis_data_conf.lock_wait_sec,
            attr=self.ctx.to_dict(),
        )

    async def __renew(self):
        co1 = self.__retrying_nums.renew()
        co2 = self.__success_nums.renew()
        co3 = self.__failed_nums.renew()
        await asyncio.gather(co1, co2, co3)

    def seg_task_name(self, sub_name: str, num: int) -> str:
        return f"{SEG_TASK_PREFIX}:{self._live.id}:{sub_name}:{num}"
