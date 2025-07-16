import asyncio
import os
import tarfile
import time

from aiofiles import os as aos
from pyutils import log, path_join, filename, error_dict, dirpath
from streamlink.stream.hls.hls import HLSStream

from .stream_types import RecordingContext
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..schema.recording_schema import RecordingState
from ...common import PlatformType
from ...data.live import LiveState
from ...fetcher import PlatformFetcher, LiveInfo
from ...file import ObjectWriter
from ...utils import random_string, FIREFOX_USER_AGENT, stem, get_streams

OBJECT_TASK_NAME = "object"
FILE_WAIT_SEC = 2


class StreamHelper:
    def __init__(
        self,
        args: RecordingArgs,
        state: RecordingState,
        writer: ObjectWriter,
        fetcher: PlatformFetcher,
        incomplete_dir_path: str,
    ):
        self.__state = state
        self.__live_url = args.live_url
        self.__session_args = args.session_args

        self.__incomplete_dir_path = incomplete_dir_path
        self.__tmp_base_path = args.tmp_dir_path

        self.__wait_timeout_sec = 30
        self.__wait_delay_sec = 1
        self.__write_retry_limit = 8
        self.__write_retry_delay_sec = 0.5

        self.__writer = writer
        self.__fetcher = fetcher

        seg_size_mb: int = args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.__seg_size = seg_size_mb * 1024 * 1024
        self.__threshold_sec = FILE_WAIT_SEC

    def get_ctx(self, state: LiveState) -> RecordingContext:
        if "User-Agent" not in self.__fetcher.headers:
            self.__fetcher.headers["User-Agent"] = FIREFOX_USER_AGENT
        if state.platform_cookie is not None:
            self.__fetcher.headers["Cookie"] = state.platform_cookie

        live = LiveInfo(
            platform=state.platform,
            channel_id=state.channel_id,
            channel_name=state.channel_name,
            live_id=state.live_id,
            live_title=state.live_title,
        )

        tmp_dir_path = path_join(self.__tmp_base_path, live.platform.value, live.channel_id, state.video_name)
        out_dir_path = path_join(self.__incomplete_dir_path, live.platform.value, live.channel_id, state.video_name)

        stream_base_url = None
        if live.platform != PlatformType.TWITCH:
            stream_base_url = "/".join(state.stream_url.split("/")[:-1])
        ctx = RecordingContext(
            record_id=state.id,
            live_url=self.__live_url,
            stream_url=state.stream_url,
            stream_base_url=stream_base_url,
            video_name=state.video_name,
            location=state.location,
            headers=state.headers or {"User-Agent": FIREFOX_USER_AGENT},
            tmp_dir_path=tmp_dir_path,
            out_dir_path=out_dir_path,
            live=live,
        )

        if state.platform_cookie is not None:
            log.debug("Using Credentials", ctx.to_dict())

        return ctx

    def wait_for_live(self, ctx: RecordingContext) -> dict[str, HLSStream] | None:
        retry_cnt = 0
        start_time = time.time()
        info = {
            "platform": ctx.live.platform,
            "channel_id": ctx.live.channel_id,
        }
        while True:
            if time.time() - start_time > self.__wait_timeout_sec:
                log.error("Wait Timeout", info)
                raise
            if self.__state.abort_flag:
                log.debug("Abort Wait", info)
                return None

            try:
                streams = get_streams(self.__live_url, self.__session_args)
                if streams is not None:
                    return streams
            except Exception as e:
                err = error_dict(e)
                err["platform"] = ctx.live.platform
                err["channel_id"] = ctx.live.channel_id
                log.error("Failed to get streams", err)

            if retry_cnt == 0:
                log.info("Wait For Live", info)
            time.sleep(self.__wait_delay_sec)
            retry_cnt += 1

    async def check_segments(self, ctx: RecordingContext):
        current_time = time.time()  # do not use asyncio.get_event_loop().time() here
        result = []
        size_sum = 0
        for seg_path in await _get_seg_paths(ctx):
            stat = await aos.stat(seg_path)
            if current_time - stat.st_mtime <= self.__threshold_sec:
                continue
            size_sum += stat.st_size
            result.append(seg_path)
            if size_sum >= self.__seg_size:
                return result
        return None

    async def check_tmp_dir(self, ctx: RecordingContext):
        if not await aos.path.exists(ctx.tmp_dir_path):
            log.warn("Temporary Directory Not Found", ctx.to_dict())
            return

        # Wait for existing threads to finish
        current_task = asyncio.current_task()
        tgt_tasks = []
        for task in asyncio.all_tasks():
            if task == current_task:
                continue
            task_name = task.get_name()
            if task_name.startswith(f"{OBJECT_TASK_NAME}:{ctx.task_path()}"):
                log.debug("Wait for object write task", {"task_name": task_name})
                tgt_tasks.append(task)
        await asyncio.gather(*tgt_tasks)

        # Write remaining segments
        tgt_seg_paths = await _get_seg_paths(ctx)
        await asyncio.sleep(FILE_WAIT_SEC)
        if len(tgt_seg_paths) > 0:
            tar_path = await asyncio.to_thread(self.archive_files, tgt_seg_paths, ctx.tmp_dir_path)
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

    def start_write_segment_task(self, file_path: str, ctx: RecordingContext):
        task_name = f"{OBJECT_TASK_NAME}:{ctx.task_path()}:{stem(file_path)}"
        _ = asyncio.create_task(self.write_segment(file_path, ctx), name=task_name)

    async def write_segment(self, tmp_file_path: str, ctx: RecordingContext):
        if not await aos.path.exists(tmp_file_path):
            return

        for retry_cnt in range(self.__write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    await self.__writer.write(path_join(ctx.out_dir_path, filename(tmp_file_path)), f.read())
                break
            except Exception as e:
                err = ctx.to_err(e, with_stream_url=False)
                if retry_cnt == self.__write_retry_limit:
                    log.error("Retry Limit Exceeded: Failed to write segment", err)
                    break
                log.debug(f"retry write segment: cnt={retry_cnt}", err)
                await asyncio.sleep(self.__write_retry_delay_sec * (2**retry_cnt))

        # Remove tmp file
        if await aos.path.exists(tmp_file_path):
            await aos.remove(tmp_file_path)

    def archive_files(self, tgt_seg_paths: list[str], dir_path: str):
        out_filename = f"{stem(tgt_seg_paths[0])}_{stem(tgt_seg_paths[-1])}_{random_string(5)}.tar"
        tar_path = path_join(dir_path, out_filename)
        with tarfile.open(tar_path, "w") as tar:
            for seg_path in tgt_seg_paths:
                tar.add(seg_path, arcname=filename(seg_path))
        for seg_path in tgt_seg_paths:
            os.remove(seg_path)
        return tar_path


async def _get_seg_paths(ctx: RecordingContext) -> list[str]:
    segment_names = [file_name for file_name in await aos.listdir(ctx.tmp_dir_path) if not file_name.endswith(".tar")]
    return [path_join(ctx.tmp_dir_path, seg_name) for seg_name in sorted(segment_names, key=lambda x: int(stem(x)))]
