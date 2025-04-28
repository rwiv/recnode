import asyncio
import random

import aiofiles
from pyutils import log, path_join
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from .stream_helper import StreamHelper
from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_schema import RecordingState, RecordingStatus, RecorderStatusInfo
from ...data.live import LiveState
from ...fetcher import PlatformFetcher
from ...file import ObjectWriter
from ...utils import AsyncHttpClient, HttpRequestError


class SegmentedStreamRecorder:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
    ):
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2

        self.idx = 0
        self.done_flag = False
        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.processed_nums: set[int] = set()  # TODO: change using redis
        self.ctx: RequestContext | None = None

        self.http = AsyncHttpClient(timeout_sec=10, retry_limit=2, retry_delay_sec=0.5, use_backoff=True)
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

        # Fetch m3u8
        try:
            m3u8_text = await self.http.get_text(self.ctx.stream_url, self.ctx.headers, print_error=False)
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
            b = await self.http.get_bytes(map_url, headers=self.ctx.headers)
            async with aiofiles.open(path_join(self.ctx.tmp_dir_path, "0.ts"), "wb") as f:
                await f.write(b)

        # Process segments
        coroutines = []
        for seg in segments:
            coroutines.append(self.__process_segment(seg))
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                log.error("Error processing segment", {"error": str(result)})
                # TODO: implement error handling logic (using redis) and remove `raise`
                raise result

        # Upload segments tar
        target_segments = await self.helper.check_segments(self.ctx)
        if target_segments is not None and len(target_segments) > 0:
            tar_path = self.helper.archive(target_segments, self.ctx.tmp_dir_path)
            # coroutines require 'await', so using threads instead of asyncio
            self.helper.write_segment_thread(tar_path, self.ctx)

        self.idx = segments[-1].num

        if playlist.is_endlist:
            self.done_flag = True
            return

        # to prevent segment requests from being concentrated on a specific node
        await asyncio.sleep(random.uniform(self.min_delay_sec, self.max_delay_sec))

    async def __process_segment(self, segment: HLSSegment):
        assert self.ctx is not None

        if segment.num in self.processed_nums:
            return

        # this is used to check the logic implemented inside `SoopHLSStreamWriter`.
        # if this log is not printed for a long time, this code will be removed.
        if "preloading" in segment.uri:
            log.debug("Preloading Segment", self.ctx.to_dict())

        seg_filename = f"{segment.num}.ts"
        seg_path = path_join(self.ctx.tmp_dir_path, seg_filename)

        url = segment.uri
        if self.ctx.stream_base_url is not None:
            url = "/".join([self.ctx.stream_base_url, segment.uri])
        b = await self.http.get_bytes(url, headers=self.ctx.headers)
        async with aiofiles.open(seg_path, "wb") as f:
            await f.write(b)

        self.processed_nums.add(segment.num)

    def __close_recording(self):
        assert self.ctx is not None
        self.helper.check_tmp_dir(self.ctx)
