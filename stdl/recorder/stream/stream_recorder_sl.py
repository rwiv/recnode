import asyncio
import time

from aiofiles import os as aos
from pyutils import log, path_join
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from .stream_recorder import StreamRecorder
from ..schema.recording_arguments import RecordingArgs
from ..schema.recording_schema import RecordingStatus
from ...data.live import LiveState
from ...file import ObjectWriter
from ...metric import MetricManager
from ...utils import AsyncHttpClient


class StreamlinkStreamRecorder(StreamRecorder):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        metric: MetricManager,
        incomplete_dir_path: str,
    ):
        super().__init__(live, args, writer, metric, incomplete_dir_path)
        self.read_retry_limit = 1
        self.read_retry_delay_sec = 0.5
        self.read_buf_size = 4 * 1024 * 1024
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2

        self.idx = 0

        self.http = AsyncHttpClient(timeout_sec=10, retry_limit=2, retry_delay_sec=0.5, use_backoff=True)

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        return self.helper.wait_for_live()

    def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        info = self.ctx.to_status(
            fs_name=self.writer.fs_name,
            num=self.idx,
            status=self.status,
        )
        return info.model_dump(mode="json", by_alias=True, exclude_none=True)

    def record(self):
        self.recording_task = asyncio.create_task(self.__record(), name=f"recording:{self.live.id}")

    async def __record(self):
        self.http.set_headers(self.ctx.headers)
        await aos.makedirs(self.ctx.tmp_dir_path, exist_ok=True)

        # Start recording
        streams = self.helper.wait_for_live()
        if streams is None:
            log.error("Failed to get live streams")
            raise ValueError("Failed to get live streams")

        stream: HLSStream | None = streams.get("best")
        if stream is None:
            raise ValueError("Failed to get best stream")

        input_stream: HLSStreamReader = stream.open()
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))
        self.status = RecordingStatus.RECORDING
        self.idx = 0

        while True:
            if self.state.abort_flag:
                log.debug("Abort Stream", self.ctx.to_dict())
                break

            if input_stream.closed:
                log.debug("Stream Closed", self.ctx.to_dict())
                break

            data: bytes = b""
            is_failed = False
            for retry_cnt in range(self.read_retry_limit + 1):
                try:
                    data = input_stream.read(self.read_buf_size)
                    break
                except OSError as e:
                    log.error("Stream Read Failure", self.ctx.to_err(e))
                    is_failed = True
                    break
                except Exception as e:
                    if retry_cnt == self.read_retry_limit:
                        log.error("Stream Read Failure: Retry Limit Exceeded", self.ctx.to_err(e))
                        is_failed = True
                        break
                    log.error(f"Stream Read Error: cnt={retry_cnt}", self.ctx.to_err(e))
                    time.sleep(self.read_retry_delay_sec * (2**retry_cnt))

            if is_failed:
                log.info("Stream read failed")
                break

            if len(data) == 0:
                log.info("The length of the read data is 0")
                continue

            tmp_file_path = path_join(self.ctx.tmp_dir_path, f"{self.idx}.ts")
            with open(tmp_file_path, "ab") as f:
                f.write(data)
            self.idx += 1

            target_segments = await self.helper.check_segments(self.ctx)
            if target_segments is not None:
                tar_path = await asyncio.to_thread(self.helper.archive_files, target_segments, self.ctx.tmp_dir_path)
                self.helper.start_write_segment_task(tar_path, self.ctx)

        await self.__close_recording()
        log.info("Finish Recording", self.ctx.to_dict())
        self.is_done = True

    async def __close_recording(self, stream: HLSStreamReader | None = None):
        # Close stream
        if stream is not None:
            stream.close()
            stream.worker.join()
            stream.writer.join()
        await self.helper.check_tmp_dir(self.ctx)
