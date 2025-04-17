import os
import threading
import time
from pathlib import Path

from pyutils import log, path_join, filename, error_dict
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from .stream_listener import StreamListener
from ..schema.recording_arguments import StreamArgs
from ..schema.recording_constants import DEFAULT_SEGMENT_SIZE_MB
from ..schema.recording_schema import RecordingState, RecordingStatus
from ..stream.streamlink_utils import get_streams
from ...common.amqp import AmqpHelper
from ...common.fs import ObjectWriter

WRITE_SEGMENT_THREAD_NAME = "Thread-WriteSegment"


class StreamRecorder:
    def __init__(
        self,
        args: StreamArgs,
        incomplete_dir_path: str,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        self.url = args.info.url
        self.uid = args.info.uid
        self.platform = args.info.platform

        self.incomplete_dir_path = incomplete_dir_path
        self.tmp_base_path = args.tmp_dir_path

        self.session_args = args.session_args

        self.wait_timeout_sec = 30
        self.wait_delay_sec = 1
        self.read_retry_limit = 1
        self.read_retry_delay_sec = 0.5
        self.read_buf_size = 4 * 1024
        self.write_retry_limit = 2
        self.write_retry_delay_sec = 1

        self.idx = 0
        self.state = RecordingState()
        self.status: RecordingStatus = RecordingStatus.WAIT
        self.video_name: str | None = None

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
                log.error("Failed to get streams", self.__error_info(e))

            if retry_cnt == 0:
                log.info("Wait For Live")
            self.status = RecordingStatus.WAIT
            time.sleep(self.wait_delay_sec)
            retry_cnt += 1

    def record(self, streams: dict[str, HLSStream], vid_name: str) -> str:
        # Start AMQP consumer thread
        self.amqp_thread = threading.Thread(target=self.listener.consume)
        self.amqp_thread.name = f"Thread-RecorderListener-{self.platform.value}-{self.uid}"
        self.amqp_thread.daemon = True  # AMQP connection should be released on abnormal termination
        self.amqp_thread.start()

        self.video_name = vid_name
        out_dir_path = path_join(self.incomplete_dir_path, self.uid, vid_name)
        tmp_dir_path = path_join(self.tmp_base_path, self.uid, vid_name)
        os.makedirs(tmp_dir_path, exist_ok=True)

        input_stream: HLSStreamReader = streams["best"].open()
        self.status = RecordingStatus.RECORDING

        self.idx = 0

        log.info("Start Recording")
        while True:
            if self.state.abort_flag:
                log.info("Abort Stream")
                self.__close_recording(input_stream)
                break

            if input_stream.closed:
                log.info("Stream Closed")
                self.__close_recording()
                break

            data: bytes = b""
            is_failed = False
            for retry_cnt in range(self.read_retry_limit + 1):
                try:
                    data = input_stream.read(self.read_buf_size)
                    break
                except OSError as e:
                    log.error("Stream Read Failure", self.__error_info(e))
                    is_failed = True
                    break
                except Exception as e:
                    if retry_cnt == self.read_retry_limit:
                        log.error("Stream Read Failure: Retry Limit Exceeded", self.__error_info(e))
                        is_failed = True
                        break
                    log.error(f"Stream Read Error: cnt={retry_cnt}", self.__error_info(e))
                    time.sleep(self.read_retry_delay_sec * (2**retry_cnt))

            if is_failed:
                log.info("Stream read failed")
                self.__close_recording(input_stream)
                break

            if len(data) == 0:
                log.info("The length of the read data is 0")
                continue

            tmp_file_path = path_join(tmp_dir_path, f"{self.idx}.ts")
            with open(tmp_file_path, "ab") as f:
                f.write(data)
            if Path(tmp_file_path).stat().st_size > self.seg_size:
                thread = threading.Thread(target=self.__write_segment, args=(tmp_file_path, out_dir_path))
                thread.name = f"{WRITE_SEGMENT_THREAD_NAME}:{self.uid}:{vid_name}:{self.idx}"
                thread.start()
                self.idx += 1

        return out_dir_path

    def __close_recording(self, stream: HLSStreamReader | None = None):
        # Close AMQP connection
        conn = self.listener.conn
        if conn is not None:

            def close_conn():
                self.listener.amqp.close(conn)

            conn.add_callback_threadsafe(close_conn)
        if self.amqp_thread is not None:
            self.amqp_thread.join()

        # Close stream
        if stream is not None:
            stream.close()
            stream.worker.join()
            stream.writer.join()

        self.check_tmp_dir()
        self.status = RecordingStatus.DONE

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
