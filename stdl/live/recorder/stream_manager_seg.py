import asyncio
import os
import random
import tarfile
import threading
import time
from pathlib import Path

import aiofiles
from aiofiles import os as aioos
from pydantic import BaseModel
from pyutils import log, path_join, filename, error_dict
from streamlink.stream.hls.hls import HLSStream
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
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
from ...utils import AsyncHttpClient, random_string

WRITE_SEGMENT_THREAD_NAME = "Thread-WriteSegment"


class RequestContext(BaseModel):
    stream_url: str
    stream_base_url: str | None
    headers: dict[str, str]
    tmp_dir_path: str
    out_dir_path: str
    live: LiveInfo

    def to_err(self, ex: Exception):
        err = error_dict(ex)
        err["stream_url"] = self.stream_url
        err["tmp_dir_path"] = self.tmp_dir_path
        self.live.set_dict(err)
        return err

    def to_dict(self):
        result = {
            "stream_url": self.stream_url,
            "tmp_dir_path": self.tmp_dir_path,
        }
        for key, value in self.live.model_dump(mode="json").items():
            result[key] = value
        return result

    def get_thread_path(self):
        return f"{self.live.platform.value}:{self.live.channel_id}:{self.live.live_id}"


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
        self.platform = args.info.platform

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

        self.session_args = args.session_args

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        self.write_retry_limit = 2
        self.write_retry_delay_sec = 1
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2

        self.idx = 0
        self.done_flag = False
        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.processed_nums: set[int] = set()  # TODO: change using redis
        self.ctx: RequestContext | None = None

        self.http = AsyncHttpClient(retry_limit=2, retry_delay_sec=0.5, use_backoff=True)
        self.writer = writer
        self.listener = StreamListener(args.info, self.state, amqp_helper)
        self.amqp_thread: threading.Thread | None = None

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
                log.error("Failed to get streams", error_dict(e))

            if retry_cnt == 0:
                log.info("Wait For Live")
            self.status = RecordingStatus.WAIT
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    async def record(self, streams: dict[str, HLSStream]):
        live = await self.__fetcher.fetch_live_info(self.url)
        if live is None:
            raise ValueError("Channel not live")

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

        self.ctx = RequestContext(
            stream_url=stream_url,
            stream_base_url="/".join(stream_url.split("/")[:-1]),
            headers=headers,
            tmp_dir_path=tmp_dir_path,
            out_dir_path=out_dir_path,
            live=live,
        )
        if live.platform == PlatformType.TWITCH:
            self.ctx.stream_base_url = None

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
        if self.ctx is None:
            raise ValueError("Context is None")
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
                # TODO: implement handle error (using rabbitmq) and remove raise
                raise result

        if playlist.is_endlist:
            self.done_flag = True
            return

        target_segments = await self.__check_segments()
        if target_segments is not None:
            tar_path = _archive(target_segments, self.ctx.tmp_dir_path)
            thread = threading.Thread(target=self.__write_segment, args=(tar_path,))
            thread.name = f"{WRITE_SEGMENT_THREAD_NAME}:{self.ctx.get_thread_path()}:{Path(tar_path).stem}"
            thread.start()

        self.idx = segments[-1].num
        # to prevent segment requests from concentrating on a specific node
        await asyncio.sleep(random.uniform(self.min_delay_sec, self.max_delay_sec))

    async def __process_segment(self, segment: HLSSegment):
        assert self.ctx is not None

        if segment.num in self.processed_nums:
            return

        seg_filename = f"{segment.num}.ts"
        seg_path = path_join(self.ctx.tmp_dir_path, seg_filename)

        url = segment.uri
        if self.ctx.stream_base_url is not None:
            url = "/".join([self.ctx.stream_base_url, segment.uri])
        b = await self.http.get_bytes(url, headers=self.ctx.headers)
        async with aiofiles.open(seg_path, "wb") as f:
            await f.write(b)

        self.processed_nums.add(segment.num)

    async def __check_segments(self):
        assert self.ctx is not None
        segment_names = [
            file_name
            for file_name in await aioos.listdir(self.ctx.tmp_dir_path)
            if not file_name.endswith(".tar")
        ]
        segment_paths = [
            path_join(self.ctx.tmp_dir_path, seg_name)
            for seg_name in sorted(segment_names, key=lambda x: int(Path(x).stem))
        ]
        result = []
        size_sum = 0
        for seg_path in segment_paths:
            size_sum += Path(seg_path).stat().st_size
            result.append(seg_path)
            if size_sum >= self.seg_size:
                return result
        return None

    def __close_recording(self):
        conn = self.listener.conn
        if conn is not None:

            def close_conn():
                self.listener.amqp.close(conn)

            conn.add_callback_threadsafe(close_conn)
        if self.amqp_thread is not None:
            self.amqp_thread.join()

        self.check_tmp_dir()

    def check_tmp_dir(self):
        assert self.ctx is not None
        # Wait for existing threads to finish
        pending_write_threads = [
            th
            for th in threading.enumerate()
            if th.name.startswith(f"{WRITE_SEGMENT_THREAD_NAME}:{self.ctx.get_thread_path()}")
        ]
        for th in pending_write_threads:
            log.info("Wait For Thread", {"thread_name": th.name})
            th.join()

        # Write remaining segments
        target_segments = [
            path_join(self.ctx.tmp_dir_path, file_name) for file_name in os.listdir(self.ctx.tmp_dir_path)
        ]
        if len(target_segments) > 0:
            tar_path = _archive(target_segments, self.ctx.tmp_dir_path)
            log.info("Detect And Write Segment", {"file_name": tar_path})
            self.__write_segment(tar_path)

        # Clear tmp dir
        if len(os.listdir(self.ctx.tmp_dir_path)) == 0:
            os.rmdir(self.ctx.tmp_dir_path)
        tmp_channel_dir_path = Path(self.ctx.tmp_dir_path).parent
        if len(os.listdir(tmp_channel_dir_path)) == 0:
            os.rmdir(tmp_channel_dir_path)

    def __write_segment(self, tmp_file_path: str):
        assert self.ctx is not None
        if not Path(tmp_file_path).exists():
            return

        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    self.writer.write(path_join(self.ctx.out_dir_path, filename(tmp_file_path)), f.read())
                log.debug("Write Segment", {"idx": filename(tmp_file_path)})
                break
            except Exception as e:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", self.ctx.to_err(e))
                    break
                log.error(f"Write Segment: cnt={retry_cnt}", self.ctx.to_err(e))
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))

        if Path(tmp_file_path).exists():
            os.remove(tmp_file_path)


def _archive(target_segments: list[str], dir_path: str):
    out_filename = f"{Path(target_segments[0]).stem}_{Path(target_segments[-1]).stem}_{random_string(5)}.tar"
    tar_path = path_join(dir_path, out_filename)
    with tarfile.open(tar_path, "w") as tar:
        for target_segment in target_segments:
            tar.add(target_segment, arcname=Path(target_segment).name)
    for target_segment in target_segments:
        os.remove(target_segment)
    return tar_path
