import asyncio
import os
import random
import threading
from pathlib import Path

import aiofiles
from pyutils import log, path_join
from streamlink.stream.hls.hls import HLSStream
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from .stream_helper import StreamHelper
from .stream_listener import StreamListener
from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_schema import RecordingState, RecordingStatus, RecorderStatusInfo
from ...common.amqp import AmqpHelper
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType
from ...fetcher import PlatformFetcher
from ...utils import AsyncHttpClient


class SegmentedStreamRecorder:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        self.url = args.info.url
        self.platform = args.info.platform

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

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
        self.listener = StreamListener(args.info, self.state, amqp_helper)
        self.amqp_thread: threading.Thread | None = None
        self.helper = StreamHelper(args, self.state, self.status, writer)

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        return self.helper.wait_for_live()

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

    async def record(self, streams: dict[str, HLSStream], video_name: str):
        # Get hls stream
        stream: HLSStream | None = streams.get("best")
        if stream is None:
            raise ValueError("Failed to get best stream")

        # Set http session context
        stream_url = stream.url
        headers = {}
        for k, v in stream.session.http.headers.items():
            headers[k] = v

        self.http.set_headers(headers)
        self.fetcher.set_headers(headers)

        live = await self.fetcher.fetch_live_info(self.url)
        if live is None:
            raise ValueError("Channel not live")

        out_dir_path = path_join(self.incomplete_dir_path, live.platform.value, live.channel_id, video_name)
        tmp_dir_path = path_join(self.tmp_base_path, live.platform.value, live.channel_id, video_name)
        os.makedirs(tmp_dir_path, exist_ok=True)

        self.ctx = RequestContext(
            stream_url=stream_url,
            stream_base_url="/".join(stream_url.split("/")[:-1]),
            video_name=video_name,
            headers=headers,
            tmp_dir_path=tmp_dir_path,
            out_dir_path=out_dir_path,
            live=live,
        )
        if live.platform == PlatformType.TWITCH:
            self.ctx.stream_base_url = None

        # Start AMQP consumer thread
        self.amqp_thread = threading.Thread(target=self.listener.consume)
        self.amqp_thread.name = f"Thread-RecorderListener-{self.platform.value}-{live.channel_id}"
        self.amqp_thread.daemon = True  # AMQP connection should be released on abnormal termination
        self.amqp_thread.start()

        # Start recording
        log.info("Start Recording", self.ctx.to_dict())
        self.status = RecordingStatus.RECORDING

        try:
            await self.__interval(is_init=True)

            while True:
                if self.done_flag:
                    self.status = RecordingStatus.DONE
                    log.info("Finish Stream")
                    break
                if self.state.abort_flag:
                    self.status = RecordingStatus.DONE
                    log.info("Abort Stream")
                    break

                await self.__interval()
        except Exception as e:
            log.error("Error during recording", self.ctx.to_err(e))
            self.status = RecordingStatus.FAILED

        self.__close_recording()
        return live

    async def __interval(self, is_init: bool = False):
        assert self.ctx is not None

        try:
            text = await self.http.get_text(self.ctx.stream_url, self.ctx.headers)
        except Exception as e:
            log.error("Failed to get playlist", self.ctx.to_err(e))
            raise

        playlist: M3U8 = M3U8Parser().parse(text)
        if playlist.is_master:
            raise ValueError("Expected a media playlist, got a master playlist")
        segments: list[HLSSegment] = playlist.segments
        if len(segments) == 0:
            raise ValueError("No segments found in the playlist")

        map_seg = segments[0].map
        if is_init and map_seg is not None:
            url = map_seg.uri
            if self.ctx.stream_base_url is not None:
                url = "/".join([self.ctx.stream_base_url, map_seg.uri])
            b = await self.http.get_bytes(url, headers=self.ctx.headers)
            async with aiofiles.open(path_join(self.ctx.tmp_dir_path, "0.ts"), "wb") as f:
                await f.write(b)

        coroutines = []
        for seg in segments:
            coroutines.append(self.__process_segment(seg))
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                log.error("Error processing segment", {"error": str(result)})
                # TODO: implement handling error (using rabbitmq) and remove `raise`
                raise result

        target_segments = await self.helper.check_segments(self.ctx)
        if target_segments is not None:
            tar_path = self.helper.archive(target_segments, self.ctx.tmp_dir_path)
            thread = threading.Thread(target=self.helper.write_segment, args=(tar_path, self.ctx))
            thread.name = (
                f"{self.helper.write_segment_thread_name}:{self.ctx.get_thread_path()}:{Path(tar_path).stem}"
            )
            thread.start()

        self.idx = segments[-1].num

        if playlist.is_endlist:
            self.done_flag = True
            return

        # To prevent segment requests from being concentrated on a specific node
        await asyncio.sleep(random.uniform(self.min_delay_sec, self.max_delay_sec))

    async def __process_segment(self, segment: HLSSegment):
        assert self.ctx is not None

        if segment.num in self.processed_nums:
            return

        # This is used to check the logic implemented inside `SoopHLSStreamWriter`.
        # If this log is not printed for a long time, this code will be removed.
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

        conn = self.listener.conn
        if conn is not None:

            def close_conn():
                self.listener.amqp.close(conn)

            conn.add_callback_threadsafe(close_conn)
        if self.amqp_thread is not None:
            self.amqp_thread.join()

        self.helper.check_tmp_dir(self.ctx)
