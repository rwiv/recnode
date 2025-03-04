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
from ..spec.recording_constants import STREAMLINK_RETRY_COUNT, STREAMLINK_BUFFER_SIZE, DEFAULT_SEGMENT_SIZE_MB
from ..spec.recording_status import RecordingState
from ...common.fs import ObjectWriter


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

        self.idx = 0
        self.wait_delay_sec = 1
        self.state: RecordingState = RecordingState.WAIT
        self.abort_flag = False
        self.video_name: str | None = None

        seg_size_mb: int = recorder_args.seg_size_mb or DEFAULT_SEGMENT_SIZE_MB
        self.seg_size = seg_size_mb * 1024 * 1024

    def get_streams(self) -> dict[str, HLSStream]:
        session = self.get_session()
        if self.options is not None:
            options = Options()
            for key, value in self.options.items():
                options.set(key, value)
            streams: dict[str, HLSStream] = session.streams(self.url, options=options)
        else:
            streams: dict[str, HLSStream] = session.streams(self.url)

        return streams

    def get_session(self) -> Streamlink:
        session = Streamlink()
        if self.cookies is not None:
            data: list[dict] = json.loads(self.cookies)
            for cookie in data:
                session.http.cookies.set(cookie["name"], cookie["value"])
        return session

    def wait_for_live(self) -> dict[str, HLSStream] | None:
        cnt = 0
        while True:
            if self.abort_flag:
                log.info("Abort Wait")
                return None
            try:
                streams = self.get_streams()
                if streams != {}:
                    return streams
            except:
                log.error("Failed to get streams", stacktrace_dict())

            if cnt == 0:
                log.info("Wait For Live")
            self.state = RecordingState.WAIT
            time.sleep(self.wait_delay_sec)
            cnt += 1

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
                log.info("Abort Stream")
                input_stream.close()
                input_stream.worker.join()
                input_stream.writer.join()
                self.state = RecordingState.DONE
                self.check_tmp_dir()
                break

            if input_stream.closed:
                log.info("Stream Closed")
                self.state = RecordingState.DONE
                self.check_tmp_dir()
                break

            data = b""
            for i in range(STREAMLINK_RETRY_COUNT):
                try:
                    data: bytes = input_stream.read(STREAMLINK_BUFFER_SIZE)
                    break
                except:
                    log.error(f"HTTP Error: cnt={i}", stacktrace_dict())

            if len(data) == 0:
                continue

            tmp_file_path = path_join(tmp_dir_path, f"{self.idx}.ts")
            with open(tmp_file_path, "ab") as f:
                f.write(data)
            if Path(tmp_file_path).stat().st_size > self.seg_size:
                thread = threading.Thread(target=self.__write_segment, args=(tmp_file_path, out_dir_path))
                thread.name = f"Thread-WriteSegment-{self.uid}-{vid_name}-{self.idx}"
                thread.start()
                self.idx += 1

        return out_dir_path

    def check_tmp_dir(self):
        if self.video_name is None:
            return
        out_chunks_dir_path = path_join(self.incomplete_dir_path, self.uid, self.video_name)
        tmp_chunks_dir_path = path_join(self.tmp_base_path, self.uid, self.video_name)
        for f in os.listdir(tmp_chunks_dir_path):
            log.info("Detect Segment", {"idx": f})
            self.__write_segment(path_join(tmp_chunks_dir_path, f), out_chunks_dir_path)

        if len(os.listdir(tmp_chunks_dir_path)) == 0:
            os.rmdir(tmp_chunks_dir_path)
        tmp_channel_dir_path = path_join(self.tmp_base_path, self.uid)
        if len(os.listdir(tmp_channel_dir_path)) == 0:
            os.rmdir(tmp_channel_dir_path)

    def __write_segment(self, tmp_file_path: str, out_dir_path: str):
        if not Path(tmp_file_path).exists():
            return
        with open(tmp_file_path, "rb") as f:
            self.writer.write(path_join(out_dir_path, filename(tmp_file_path)), f)
        os.remove(tmp_file_path)
        log.debug("Write Segment", {"idx": filename(tmp_file_path)})
