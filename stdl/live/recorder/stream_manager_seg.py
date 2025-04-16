import asyncio
import os
import threading
import time
from pathlib import Path

import aiofiles
from aiofiles import os as aioos
from pydantic import BaseModel
from pyutils import log, path_join, filename, error_dict
from streamlink.stream.hls.hls import HLSStream
from streamlink.stream.hls.m3u8 import M3U8Parser
from streamlink.stream.hls.segment import HLSSegment

from .stream_listener import StreamListener
from ..spec.recording_arguments import StreamArgs
from ..spec.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..spec.recording_schema import RecordingState, RecordingStatus
from ..utils.streamlink_utils import get_streams
from ...common.amqp import AmqpHelper
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType
from ...fetcher import PlatformFetcher, LiveInfo
from ...utils import AsyncHttpClient

WRITE_SEGMENT_THREAD_NAME = "Thread-WriteSegment"


class RequestContext(BaseModel):
    stream_url: str
    base_url: str | None
    headers: dict[str, str]
    dir_path: str


class SegmentedStreamManager:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        self.__fetcher = PlatformFetcher()

        self.url = args.info.url
        self.uid = args.info.uid
        self.platform = args.info.platform

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

        self.session_args = args.session_args

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        self.write_retry_limit = 2
        self.write_retry_delay_sec = 1

        self.idx = 0
        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.video_name: str | None = None

        self.writer = writer
        self.listener = StreamListener(args.info, self.state, amqp_helper)
        self.amqp_thread: threading.Thread | None = None
        self.http = AsyncHttpClient()

        seg_size_mb: int = args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.seg_size = seg_size_mb * 1024 * 1024

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        retry_cnt = 0
        start_time = time.time()
        while True:
            if time.time() - start_time > self.wait_timeout_sec:
                log.info("Wait Timeout")
                return None
            if self.state.abort_flag:
                log.info("Abort Wait")
                return None

            try:
                streams = get_streams(self.url, self.session_args)
                if streams is not None:
                    return streams
            except Exception as e:
                log.error("Failed to get streams", self.__error_info(e))

            if retry_cnt == 0:
                log.info("Wait For Live")
            self.status = RecordingStatus.WAIT
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    def check_segments(self, live: LiveInfo):
        dir_path = path_join(self.tmp_base_path, live.platform.value, self.uid, live.live_id)
        prev_filename: str | None = None
        for cur_filename in sorted(os.listdir(dir_path), key=lambda x: int(Path(x).stem)):
            if prev_filename is None:
                prev_filename = cur_filename
                continue

            prev_num = int(Path(prev_filename).stem)
            if prev_num == 0:
                prev_filename = cur_filename
                continue

            cur_num = int(Path(cur_filename).stem)
            if cur_num - prev_num != 1:
                log.error("Missing Segment", {"prev": prev_filename, "cur": cur_filename})

            prev_filename = cur_filename

    async def record(self, streams: dict[str, HLSStream]):
        live = self.__fetcher.fetch_live_info(self.url)

        # Start AMQP consumer thread
        self.amqp_thread = threading.Thread(target=self.listener.consume)
        self.amqp_thread.name = f"Thread-RecorderListener-{self.platform.value}-{live.channel_id}"
        self.amqp_thread.daemon = True  # AMQP connection should be released on abnormal termination
        self.amqp_thread.start()

        out_dir_path = path_join(self.incomplete_dir_path, live.platform.value, live.channel_id, live.live_id)
        tmp_dir_path = path_join(self.tmp_base_path, live.platform.value, live.channel_id, live.live_id)
        os.makedirs(tmp_dir_path, exist_ok=True)

        stream: HLSStream | None = streams.get("best")
        if stream is None:
            raise ValueError("Failed to get best stream")

        stream_url = stream.url
        headers = {}
        for k, v in stream.session.http.headers.items():
            headers[k] = v

        ctx = RequestContext(
            stream_url=stream_url,
            base_url="/".join(stream_url.split("/")[:-1]),
            headers=headers,
            dir_path=tmp_dir_path,
        )
        if live.platform == PlatformType.TWITCH:
            ctx.base_url = None

        log.info("Start Recording")
        self.status = RecordingStatus.RECORDING

        await self.__interval(ctx, is_init=True)

        while True:
            if self.state.abort_flag:
                log.info("Abort Stream")
                break

            await self.__interval(ctx)

        self.status = RecordingStatus.DONE
        return live

    async def __interval(self, ctx: RequestContext, is_init: bool = False):
        playlist = M3U8Parser().parse(await self.http.get_text(ctx.stream_url, ctx.headers))
        if playlist.is_master:
            raise ValueError("Expected a media playlist, got a master playlist")
        segments: list[HLSSegment] = playlist.segments
        if len(segments) == 0:
            raise ValueError("No segments found in the playlist")

        if is_init:
            map_seg = segments[0].map
            if map_seg is None:
                raise ValueError("No map segment found in the playlist")
            url = map_seg.uri
            if ctx.base_url is not None:
                url = "/".join([ctx.base_url, map_seg.uri])
            b = await self.http.get_bytes(url, headers=ctx.headers)
            async with aiofiles.open(path_join(ctx.dir_path, "0.ts"), "wb") as f:
                await f.write(b)

        coroutines = []
        for seg in segments:
            coroutines.append(self.__process_segment(seg, ctx))
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                log.error("Error processing segment", {"error": str(result)})

        await asyncio.sleep(1)

    async def __process_segment(self, segment: HLSSegment, ctx: RequestContext):
        seg_filename = f"{segment.num}.ts"
        seg_path = path_join(ctx.dir_path, seg_filename)

        if seg_filename in await aioos.listdir(ctx.dir_path):
            return

        url = segment.uri
        if ctx.base_url is not None:
            url = "/".join([ctx.base_url, segment.uri])
        b = await self.http.get_bytes(url, headers=ctx.headers)
        async with aiofiles.open(seg_path, "wb") as f:
            await f.write(b)

        self.idx = segment.num
        log.debug("Write Segment", {"idx": seg_filename})

    def __error_info(self, ex: Exception) -> dict:
        err_info = error_dict(ex)
        err_info["uid"] = self.uid
        err_info["url"] = self.url
        return err_info

    def check_tmp_dir(self):
        if self.video_name is None:
            return

        out_chunks_dir_path = path_join(self.incomplete_dir_path, self.uid, self.video_name)
        tmp_chunks_dir_path = path_join(self.tmp_base_path, self.uid, self.video_name)

        # Wait for existing threads to finish
        pending_write_threads = [
            th
            for th in threading.enumerate()
            if th.name.startswith(f"{WRITE_SEGMENT_THREAD_NAME}:{self.uid}:{self.video_name}")
        ]
        for th in pending_write_threads:
            log.info("Wait For Thread", {"thread_name": th.name})
            th.join()

        # Write remaining segments
        for file_name in os.listdir(tmp_chunks_dir_path):
            log.info("Detect And Write Segment", {"file_name": file_name})
            self.__write_segment(path_join(tmp_chunks_dir_path, file_name), out_chunks_dir_path)

        # Clear tmp dir
        if len(os.listdir(tmp_chunks_dir_path)) == 0:
            os.rmdir(tmp_chunks_dir_path)
        tmp_channel_dir_path = path_join(self.tmp_base_path, self.uid)
        if len(os.listdir(tmp_channel_dir_path)) == 0:
            os.rmdir(tmp_channel_dir_path)

    def __write_segment(self, tmp_file_path: str, out_dir_path: str):
        if not Path(tmp_file_path).exists():
            return

        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    self.writer.write(path_join(out_dir_path, filename(tmp_file_path)), f.read())
                log.debug("Write Segment", {"idx": filename(tmp_file_path)})
                break
            except Exception as e:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", self.__error_info(e))
                    break
                log.error(f"Write Segment: cnt={retry_cnt}", self.__error_info(e))
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))

        if Path(tmp_file_path).exists():
            os.remove(tmp_file_path)
