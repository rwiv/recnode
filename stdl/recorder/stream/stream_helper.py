import os
import tarfile
import threading
import time
from pathlib import Path

from aiofiles import os as aioos
from pyutils import log, path_join, filename, error_dict
from streamlink.stream.hls.hls import HLSStream

from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..schema.recording_schema import RecordingState, RecordingStatus
from ..stream.streamlink_utils import get_streams
from ...common.fs import ObjectWriter
from ...utils import random_string

WRITE_SEGMENT_THREAD_NAME = "Thread-WriteSegment"


class StreamHelper:
    def __init__(
        self,
        args: StreamArgs,
        state: RecordingState,
        status: RecordingStatus,
        writer: ObjectWriter,
    ):
        self.url = args.info.url
        self.stream_info = args.info
        self.session_args = args.session_args
        self.state = state
        self.status = status

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        self.write_retry_limit = 8
        self.write_retry_delay_sec = 0.5

        self.writer = writer

        seg_size_mb: int = args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.seg_size = seg_size_mb * 1024 * 1024

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        retry_cnt = 0
        start_time = time.time()
        info = {
            "platform": self.stream_info.platform,
            "channel_id": self.stream_info.uid,
        }
        while True:
            if time.time() - start_time > self.wait_timeout_sec:
                log.error("Wait Timeout", info)
                raise
            if self.state.abort_flag:
                log.debug("Abort Wait", info)
                return None

            try:
                streams = get_streams(self.url, self.session_args)
                if streams is not None:
                    return streams
            except Exception as e:
                err = error_dict(e)
                err["platform"] = self.stream_info.platform
                err["channel_id"] = self.stream_info.uid
                log.error("Failed to get streams", err)

            if retry_cnt == 0:
                log.info("Wait For Live", info)
            self.status = RecordingStatus.WAIT
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    async def check_segments(self, ctx: RequestContext):
        segment_names = [
            file_name for file_name in await aioos.listdir(ctx.tmp_dir_path) if not file_name.endswith(".tar")
        ]
        segment_paths = [
            path_join(ctx.tmp_dir_path, seg_name)
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

    def check_tmp_dir(self, ctx: RequestContext):
        # Wait for existing threads to finish
        pending_write_threads = [
            th
            for th in threading.enumerate()
            if th.name.startswith(f"{WRITE_SEGMENT_THREAD_NAME}:{ctx.get_thread_path()}")
        ]
        for th in pending_write_threads:
            log.debug("Wait For Thread", {"thread_name": th.name})
            th.join()

        # Write remaining segments
        target_segments = [
            path_join(ctx.tmp_dir_path, file_name) for file_name in os.listdir(ctx.tmp_dir_path)
        ]
        if len(target_segments) > 0:
            tar_path = self.archive(target_segments, ctx.tmp_dir_path)
            ctx_info = ctx.to_dict()
            ctx_info["file_name"] = tar_path
            log.debug("Detect And Write Segment", ctx_info)
            self.write_segment(tar_path, ctx)

        # Clear tmp dir
        if len(os.listdir(ctx.tmp_dir_path)) == 0:
            os.rmdir(ctx.tmp_dir_path)
        tmp_channel_dir_path = Path(ctx.tmp_dir_path).parent
        if len(os.listdir(tmp_channel_dir_path)) == 0:
            os.rmdir(tmp_channel_dir_path)

    # Coroutines require 'await', so using threads instead of asyncio
    def write_segment_thread(self, file_path: str, ctx: RequestContext) -> threading.Thread:
        thread = threading.Thread(target=self.write_segment, args=(file_path, ctx))
        thread.name = f"{WRITE_SEGMENT_THREAD_NAME}:{ctx.get_thread_path()}:{Path(file_path).stem}"
        thread.start()
        return thread

    def write_segment(self, tmp_file_path: str, ctx: RequestContext):
        if not Path(tmp_file_path).exists():
            return

        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    self.writer.write(path_join(ctx.out_dir_path, filename(tmp_file_path)), f.read())
                break
            except Exception as e:
                err = ctx.to_err(e, with_stream_url=False)
                if retry_cnt == self.write_retry_limit:
                    log.error("Retry Limit Exceeded: Failed to write segment", err)
                    break
                log.debug(f"retry write segment: cnt={retry_cnt}", err)
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))

        if Path(tmp_file_path).exists():
            os.remove(tmp_file_path)

    def archive(self, target_segments: list[str], dir_path: str):
        out_filename = (
            f"{Path(target_segments[0]).stem}_{Path(target_segments[-1]).stem}_{random_string(5)}.tar"
        )
        tar_path = path_join(dir_path, out_filename)
        with tarfile.open(tar_path, "w") as tar:
            for target_segment in target_segments:
                tar.add(target_segment, arcname=Path(target_segment).name)
        for target_segment in target_segments:
            os.remove(target_segment)
        return tar_path
