import asyncio
import json
import random

import aiofiles
from pyutils import log, path_join, error_dict
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from .stream_helper import StreamHelper
from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_schema import RecordingState, RecordingStatus, RecorderStatusInfo
from ...data.live import LiveState
from ...fetcher import PlatformFetcher
from ...file import ObjectWriter
from ...utils import AsyncHttpClient, HttpRequestError, AsyncMap, AsyncSet

DEFAULT_SEGMENT_LIMIT = 2
FAILED_CNT_THRESHOLD_RATIO = 2


class Segment:
    def __init__(self, num: int, url: str, limit: int = DEFAULT_SEGMENT_LIMIT):
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
        segment_limit: int = DEFAULT_SEGMENT_LIMIT,
    ):
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2
        self.segment_limit = segment_limit

        self.idx = 0
        self.done_flag = False
        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.processing_nums: AsyncSet[int] = AsyncSet()
        self.success_nums: AsyncSet[int] = AsyncSet()
        self.failed_segments: AsyncMap[int, Segment] = AsyncMap()
        self.request_counts: AsyncMap[int, int] = AsyncMap()
        self.ctx: RequestContext | None = None

        self.http = AsyncHttpClient(timeout_sec=5, retry_limit=2, retry_delay_sec=0.5, use_backoff=True)
        self.seg_http = AsyncHttpClient(timeout_sec=5, retry_limit=0, retry_delay_sec=0)
        self.fetcher = PlatformFetcher()
        self.writer = writer
        self.helper = StreamHelper(
            args, self.state, self.status, writer, self.fetcher, incomplete_dir_path=incomplete_dir_path
        )

    def check_tmp_dir(self):
        assert self.ctx is not None
        self.helper.check_tmp_dir(self.ctx)

    def get_status(self) -> RecorderStatusInfo:
        assert self.ctx is not None
        return self.ctx.to_status(
            fs_name=self.writer.fs_name,
            num=self.idx,
            status=self.status,
        )

    async def record(self, state: LiveState):
        self.ctx = await self.helper.get_ctx(state)
        self.http.set_headers(self.ctx.headers)
        self.seg_http.set_headers(self.ctx.headers)

        # Start recording
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))
        self.status = RecordingStatus.RECORDING

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

        self.__close_recording()
        log.info("Finish Recording", self.ctx.to_dict())
        return self.ctx.live

    async def __interval(self, is_init: bool = False):
        assert self.ctx is not None

        # stats = json.dumps(
        #     {
        #         "processing": self.processing_nums.list(),
        #         "success": self.success_nums.list(),
        #         "failed": self.failed_segments.keys(),
        #         "cnt": self.request_counts.map,
        #     },
        #     indent=4,
        # )
        # print(stats)

        # Fetch m3u8
        try:
            m3u8_text = await self.http.get_text(
                self.ctx.stream_url, self.ctx.headers, attr=self.ctx.to_dict(), print_error=False
            )
        except HttpRequestError as e:
            live_info = await self.fetcher.fetch_live_info(self.ctx.live_url)
            if live_info is None:
                self.done_flag = True
                return
            else:
                log.error("Failed to get playlist", self.ctx.to_err(e))
                raise
        except Exception as e:
            log.error("Failed to get playlist", self.ctx.to_err(e))
            raise

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
        for seg in segments:
            if self.success_nums.contains(seg.num) or self.processing_nums.contains(seg.num):
                continue
            # add to processing list
            await self.processing_nums.add(seg.num)

            url = seg.uri
            if self.ctx.stream_base_url is not None:
                url = "/".join([self.ctx.stream_base_url, seg.uri])
            _ = asyncio.create_task(self.__process_segment(Segment(num=seg.num, url=url)))

        for _, failed in self.failed_segments.items():
            if self.success_nums.contains(failed.num):
                continue
            if not await failed.acquire():
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

    async def __process_segment(self, seg: Segment):
        assert self.ctx is not None

        # Update request count
        req_cnt = await self.request_counts.get(seg.num)
        if req_cnt is None:
            req_cnt = 0
        req_cnt += 1
        await self.request_counts.set(seg.num, req_cnt)

        if "preloading" in seg.url:
            # this is used to check the logic implemented inside `SoopHLSStreamWriter`.
            # if this log is not printed for a long time, this code will be removed.
            log.debug("Preloading Segment", self.ctx.to_dict())

        try:
            await asyncio.sleep(2)
            # if random.random() < 0.5:  # TODO: remove (only for test)
            #     raise ValueError("Simulated error")

            b = await self.seg_http.get_bytes(
                seg.url, headers=self.ctx.headers, attr=self.ctx.to_dict({"num": seg.num})
            )
            seg_path = path_join(self.ctx.tmp_dir_path, f"{seg.num}.ts")
            async with aiofiles.open(seg_path, "wb") as f:
                await f.write(b)

            await self.success_nums.add(seg.num)
            await self.processing_nums.remove(seg.num)
            if seg.is_failed:
                await self.failed_segments.remove(seg.num)
        except BaseException as ex:
            if not seg.is_failed:  # first time failed:
                seg.is_failed = True
                await self.failed_segments.set(seg.num, seg)
            else:  # case of retry:
                if req_cnt >= (self.segment_limit * FAILED_CNT_THRESHOLD_RATIO):
                    await self.processing_nums.remove(seg.num)
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
