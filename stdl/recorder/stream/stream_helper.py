import asyncio
import os
import tarfile
import time

from aiofiles import os as aos
from pyutils import log, path_join, filename, error_dict, dirpath
from streamlink.stream.hls.hls import HLSStream

from .stream_types import RequestContext
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..schema.recording_schema import RecordingState
from ..stream.streamlink_utils import get_streams
from ...common import PlatformType
from ...data.live import LiveState
from ...fetcher import PlatformFetcher, LiveInfo
from ...file import AsyncObjectWriter
from ...utils import random_string, FIREFOX_USER_AGENT, stem

OBJECT_TASK_NAME = "object"
FILE_WAIT_SEC = 2


class StreamHelper:
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        state: RecordingState,
        writer: AsyncObjectWriter,
        fetcher: PlatformFetcher,
        incomplete_dir_path: str,
    ):
        self.live = live
        self.live_url = args.live_url
        self.session_args = args.session_args
        self.state = state

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        self.write_retry_limit = 8
        self.write_retry_delay_sec = 0.5

        self.writer = writer
        self.fetcher = fetcher

        seg_size_mb: int = args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.seg_size = seg_size_mb * 1024 * 1024
        self.threshold_sec = FILE_WAIT_SEC

    def get_ctx(self, state: LiveState) -> RequestContext:
        headers = {"User-Agent": FIREFOX_USER_AGENT}
        if state.headers is not None:
            for k, v in state.headers.items():
                headers[k] = v
        if len(self.fetcher.headers) == 0:
            self.fetcher.set_headers(headers)

        live = LiveInfo(
            platform=state.platform,
            channel_id=state.channel_id,
            channel_name=state.channel_name,
            live_id=state.live_id,
            live_title=state.live_title,
        )

        tmp_dir_path = path_join(self.tmp_base_path, live.platform.value, live.channel_id, state.video_name)
        out_dir_path = path_join(self.incomplete_dir_path, live.platform.value, live.channel_id, state.video_name)

        stream_base_url = None
        if live.platform != PlatformType.TWITCH:
            stream_base_url = "/".join(state.stream_url.split("/")[:-1])
        ctx = RequestContext(
            id=state.id,
            live_url=self.live_url,
            stream_url=state.stream_url,
            stream_base_url=stream_base_url,
            video_name=state.video_name,
            headers=headers,
            tmp_dir_path=tmp_dir_path,
            out_dir_path=out_dir_path,
            live=live,
        )

        if headers.get("Cookie") is not None:
            log.debug("Using Credentials", ctx.to_dict())

        return ctx

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        retry_cnt = 0
        start_time = time.time()
        info = {
            "platform": self.live.platform.value,
            "channel_id": self.live.channel_id,
        }
        while True:
            if time.time() - start_time > self.wait_timeout_sec:
                log.error("Wait Timeout", info)
                raise
            if self.state.abort_flag:
                log.debug("Abort Wait", info)
                return None

            try:
                streams = get_streams(self.live_url, self.session_args)
                if streams is not None:
                    return streams
            except Exception as e:
                err = error_dict(e)
                err["platform"] = self.live.platform
                err["channel_id"] = self.live.channel_id
                log.error("Failed to get streams", err)

            if retry_cnt == 0:
                log.info("Wait For Live", info)
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    async def check_segments(self, ctx: RequestContext):
        segment_names = [
            file_name for file_name in await aos.listdir(ctx.tmp_dir_path) if not file_name.endswith(".tar")
        ]
        segment_paths = [
            path_join(ctx.tmp_dir_path, seg_name) for seg_name in sorted(segment_names, key=lambda x: int(stem(x)))
        ]

        current_time = time.time()  # do not use asyncio.get_event_loop().time() here
        result = []
        size_sum = 0
        for seg_path in segment_paths:
            stat = await aos.stat(seg_path)
            if current_time - stat.st_mtime <= self.threshold_sec:
                continue
            size_sum += stat.st_size
            result.append(seg_path)
            if size_sum >= self.seg_size:
                return result
        return None

    async def check_tmp_dir(self, ctx: RequestContext):
        # Wait for existing threads to finish
        current_task = asyncio.current_task()
        tg_tasks = []
        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            task_name = task.get_name()
            if task_name.startswith(f"{OBJECT_TASK_NAME}:{ctx.task_path()}"):
                log.debug("Wait for object write task", {"task_name": task_name})
                tg_tasks.append(task)
        await asyncio.gather(*tg_tasks)

        # Write remaining segments
        target_segments = [path_join(ctx.tmp_dir_path, file_name) for file_name in await aos.listdir(ctx.tmp_dir_path)]
        await asyncio.sleep(FILE_WAIT_SEC)
        if len(target_segments) > 0:
            tar_path = await asyncio.to_thread(self.archive_files, target_segments, ctx.tmp_dir_path)
            ctx_info = ctx.to_dict()
            ctx_info["file_name"] = tar_path
            log.debug("Detect And Write Segment", ctx_info)
            await self.write_segment(tar_path, ctx)

        # Clear tmp dir
        if len(await aos.listdir(ctx.tmp_dir_path)) == 0:
            await aos.rmdir(ctx.tmp_dir_path)
        tmp_channel_dir_path = dirpath(ctx.tmp_dir_path)
        if len(await aos.listdir(tmp_channel_dir_path)) == 0:
            await aos.rmdir(tmp_channel_dir_path)

    def start_write_segment_task(self, file_path: str, ctx: RequestContext):
        task_name = f"{OBJECT_TASK_NAME}:{ctx.task_path()}:{stem(file_path)}"
        _ = asyncio.create_task(self.write_segment(file_path, ctx), name=task_name)

    async def write_segment(self, tmp_file_path: str, ctx: RequestContext):
        if not await aos.path.exists(tmp_file_path):
            return

        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    await self.writer.write(path_join(ctx.out_dir_path, filename(tmp_file_path)), f.read())
                break
            except Exception as e:
                err = ctx.to_err(e, with_stream_url=False)
                if retry_cnt == self.write_retry_limit:
                    log.error("Retry Limit Exceeded: Failed to write segment", err)
                    break
                log.debug(f"retry write segment: cnt={retry_cnt}", err)
                await asyncio.sleep(self.write_retry_delay_sec * (2**retry_cnt))

        # Remove tmp file
        if await aos.path.exists(tmp_file_path):
            await aos.remove(tmp_file_path)

    def archive_files(self, target_segments: list[str], dir_path: str):
        out_filename = f"{stem(target_segments[0])}_{stem(target_segments[-1])}_{random_string(5)}.tar"
        tar_path = path_join(dir_path, out_filename)
        with tarfile.open(tar_path, "w") as tar:
            for target_segment in target_segments:
                tar.add(target_segment, arcname=filename(target_segment))
        for target_segment in target_segments:
            os.remove(target_segment)
        return tar_path
