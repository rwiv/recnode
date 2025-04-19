import os
import time

from pyutils import log, path_join
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from .stream_helper import StreamHelper
from .stream_types import RequestContext
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_schema import RecordingState, RecordingStatus, RecorderStatusInfo
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType
from ...fetcher import PlatformFetcher
from ...utils import AsyncHttpClient


class StreamlinkStreamRecorder:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
    ):
        self.url = args.info.url
        self.platform = args.info.platform

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

        self.read_retry_limit = 1
        self.read_retry_delay_sec = 0.5
        self.read_buf_size = 4 * 1024 * 1024
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

        # Start recording
        input_stream: HLSStreamReader = stream.open()
        log.info("Start Recording", self.ctx.to_dict())
        self.status = RecordingStatus.RECORDING

        self.idx = 0

        log.info("Start Recording")
        while True:
            if self.state.abort_flag:
                log.info("Abort Stream")
                break

            if input_stream.closed:
                log.info("Stream Closed")
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

            tmp_file_path = path_join(tmp_dir_path, f"{self.idx}.ts")
            with open(tmp_file_path, "ab") as f:
                f.write(data)
            self.idx += 1

            target_segments = await self.helper.check_segments(self.ctx)
            if target_segments is not None:
                tar_path = self.helper.archive(target_segments, self.ctx.tmp_dir_path)
                # Coroutines require 'await', so using threads instead of asyncio
                self.helper.write_segment_thread(tar_path, self.ctx)

        self.__close_recording()
        return live

    def __close_recording(self, stream: HLSStreamReader | None = None):
        assert self.ctx is not None

        # Close stream
        if stream is not None:
            stream.close()
            stream.worker.join()
            stream.writer.join()

        self.helper.check_tmp_dir(self.ctx)
