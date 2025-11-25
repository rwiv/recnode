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
from ...utils import AsyncHttpClient, ProxyConnectorConfig


class StreamlinkStreamRecorder(StreamRecorder):
    def __init__(
        self,
        live: LiveState,
        args: RecordingArgs,
        writer: ObjectWriter,
        incomplete_dir_path: str,
        proxy: ProxyConnectorConfig | None,
    ):
        super().__init__(live, args, writer, incomplete_dir_path, proxy)
        self.read_retry_limit = 1
        self.read_retry_delay_sec = 0.5
        self.read_buf_size = 4 * 1024 * 1024
        self.min_delay_sec = 0.7
        self.max_delay_sec = 1.2

        self.idx = 0

        self.http = AsyncHttpClient(
            timeout_sec=10,
            retry_limit=2,
            retry_delay_sec=0.5,
            use_backoff=True,
            proxy=proxy,
        )

    async def get_status(self, with_stats: bool = False, full_stats: bool = False) -> dict:
        info = self.ctx.to_status(
            fs_name=self._writer.fs_name,
            num=self.idx,
            status=self._status,
        )
        return info.model_dump(mode="json", by_alias=True)

    async def _record(self):
        self.http.set_headers(self.ctx.stream_headers)
        await aos.makedirs(self.ctx.tmp_dir_path, exist_ok=True)

        # Start recording
        streams = self._helper.wait_for_live(self.ctx)
        if streams is None:
            log.error("Failed to get live streams")
            raise ValueError("Failed to get live streams")

        stream: HLSStream | None = streams.get("best")
        if stream is None:
            raise ValueError("Failed to get best stream")

        input_stream: HLSStreamReader = stream.open()
        log.info("Start Recording", self.ctx.to_dict(with_stream_url=True))
        self._status = RecordingStatus.RECORDING
        self.idx = 0

        while True:
            if self._state.abort_flag:
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

            tgt_seg_paths = await self._helper.check_segments(self.ctx)
            if tgt_seg_paths is not None:
                tar_path = await asyncio.to_thread(self._helper.archive_files, tgt_seg_paths, self.ctx.tmp_dir_path)
                self._helper.start_write_segment_task(tar_path, self.ctx)

        await self.__close_recording()
        log.info("Finish Recording", self.ctx.to_dict())
        self.is_done = True

    async def __close_recording(self, stream: HLSStreamReader | None = None):
        # Close stream
        if stream is not None:
            stream.close()
            stream.worker.join()
            stream.writer.join()
        await self._helper.check_tmp_dir(self.ctx)
