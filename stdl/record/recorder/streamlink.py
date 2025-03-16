import json
import os
import threading
import time
from pathlib import Path

from pyutils import stacktrace_dict, log, path_join, filename
from streamlink.options import Options
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ..spec.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..spec.recording_status import RecordingState
from ...common.fs import ObjectWriter


WRITE_SEGMENT_THREAD_NAME = "Thread-WriteSegment"


class StreamlinkManager:
    def __init__(
        self,
        stream_args: StreamlinkArgs,
        recorder_args: RecorderArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
    ):
        self.url = stream_args.url
        self.uid = stream_args.uid
        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = recorder_args.tmp_dir_path
        self.cookies = stream_args.cookies
        self.options = stream_args.options
        self.writer = writer

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        # When a read timeout occurs, a retry is triggered, but the timeout is 1 minute
        # (Read timeout occurs when the internet connection is unstable)
        self.read_retry_limit = 1
        self.read_retry_delay_sec = 0.5
        self.read_session_timeout = 30
        self.read_buf_size = 4 * 1024
        self.write_retry_limit = 2
        self.write_retry_delay_sec = 1

        self.idx = 0
        self.state: RecordingState = RecordingState.WAIT
        self.abort_flag = False
        self.video_name: str | None = None

        seg_size_mb: int = recorder_args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.seg_size = seg_size_mb * 1024 * 1024

    def get_session(self) -> Streamlink:
        options = Options()
        options.set("stream-timeout", self.read_session_timeout)
        if self.options is not None:
            for key, value in self.options.items():
                options.set(key, value)

        session = Streamlink(options=options)
        if self.cookies is not None:
            data: list[dict] = json.loads(self.cookies)
            for cookie in data:
                session.http.cookies.set(cookie["name"], cookie["value"])
        return session

    def get_streams(self) -> dict[str, HLSStream] | None:
        streams = self.get_session().streams(self.url)
        if streams is None or len(streams) == 0:
            return None
        return streams

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        retry_cnt = 0
        start_time = time.time()
        while True:
            if time.time() - start_time > self.wait_timeout_sec:
                log.info("Wait Timeout")
                return None
            if self.abort_flag:
                log.info("Abort Wait")
                return None

            try:
                streams = self.get_streams()
                if streams is not None:
                    return streams
            except:
                log.error("Failed to get streams", self.__get_stacktrace())

            if retry_cnt == 0:
                log.info("Wait For Live")
            self.state = RecordingState.WAIT
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    def record(self, streams: dict[str, HLSStream], vid_name: str) -> str:
        self.video_name = vid_name
        out_dir_path = path_join(self.incomplete_dir_path, self.uid, vid_name)
        tmp_dir_path = path_join(self.tmp_base_path, self.uid, vid_name)
        os.makedirs(tmp_dir_path, exist_ok=True)

        input_stream: HLSStreamReader = streams["best"].open()
        self.state = RecordingState.RECORDING

        self.idx = 0

        log.info("Start Recording")
        while True:
            if self.abort_flag:
                close_stream(input_stream)
                self.__close_recording("Abort Stream")
                break

            if input_stream.closed:
                self.__close_recording("Stream Closed")
                break

            data: bytes = b""
            is_failed = False
            for retry_cnt in range(self.read_retry_limit + 1):
                try:
                    data = input_stream.read(self.read_buf_size)
                    break
                except OSError:
                    log.error("Stream Read Failure", self.__get_stacktrace())
                    is_failed = True
                    break
                except:
                    if retry_cnt == self.read_retry_limit:
                        log.error("Stream Read Failure: Retry Limit Exceeded", self.__get_stacktrace())
                        is_failed = True
                        break
                    log.error(f"Stream Read Error: cnt={retry_cnt}", self.__get_stacktrace())
                    time.sleep(self.read_retry_delay_sec * (2**retry_cnt))

            if is_failed or len(data) == 0:
                close_stream(input_stream)
                self.__close_recording("Stream read failed")
                break

            tmp_file_path = path_join(tmp_dir_path, f"{self.idx}.ts")
            with open(tmp_file_path, "ab") as f:
                f.write(data)
            if Path(tmp_file_path).stat().st_size > self.seg_size:
                thread = threading.Thread(target=self.__write_segment, args=(tmp_file_path, out_dir_path))
                thread.name = f"{WRITE_SEGMENT_THREAD_NAME}:{self.uid}:{vid_name}:{self.idx}"
                thread.start()
                self.idx += 1

        return out_dir_path

    def __close_recording(self, message: str):
        self.state = RecordingState.DONE
        self.check_tmp_dir()
        log.info(message)

    def __get_stacktrace(self) -> dict:
        dct = stacktrace_dict()
        dct["uid"] = self.uid
        dct["url"] = self.url
        return dct

    def check_tmp_dir(self):
        if self.video_name is None:
            return
        out_chunks_dir_path = path_join(self.incomplete_dir_path, self.uid, self.video_name)
        tmp_chunks_dir_path = path_join(self.tmp_base_path, self.uid, self.video_name)
        thread_video_indices = [
            th.name.split(":")[3]
            for th in threading.enumerate()
            if th.name.startswith(f"{WRITE_SEGMENT_THREAD_NAME}:{self.uid}:{self.video_name}")
        ]
        for file_name in os.listdir(tmp_chunks_dir_path):
            if file_name.split(".")[0] in thread_video_indices:
                log.info("Detect And Skip Segment", {"file_name": file_name})
                continue
            log.info("Detect And Write Segment", {"file_name": file_name})
            self.__write_segment(path_join(tmp_chunks_dir_path, file_name), out_chunks_dir_path)

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
            except:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", self.__get_stacktrace())
                    break
                log.error(f"Write Segment: cnt={retry_cnt}", self.__get_stacktrace())
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))

        if Path(tmp_file_path).exists():
            os.remove(tmp_file_path)


def close_stream(input_stream: HLSStreamReader):
    input_stream.close()
    input_stream.worker.join()
    input_stream.writer.join()
