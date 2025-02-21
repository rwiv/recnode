import json
import threading
import time

from pyutils import stacktrace_dict, log
from streamlink.options import Options
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from ..spec.recording_arguments import StreamlinkArgs
from ..spec.recording_constants import STREAMLINK_RETRY_COUNT, STREAMLINK_BUFFER_SIZE
from ..spec.recording_status import RecordingState
from ...common.fs import FsAccessor


class StreamlinkManager:
    def __init__(self, args: StreamlinkArgs, out_dir_path: str, ac: FsAccessor):
        self.url = args.url
        self.uid = args.uid
        self.out_dir_path = out_dir_path
        self.cookies = args.cookies
        self.options = args.options
        self.ac = ac

        self.idx = 0
        self.wait_delay_sec = 1
        self.state: RecordingState = RecordingState.WAIT
        self.abort_flag = False

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
        out_dir_path = f"{self.out_dir_path}/{self.uid}/{vid_name}"

        input_stream: HLSStreamReader = streams["best"].open()
        self.state = RecordingState.RECORDING

        self.idx = 0

        log.info("Start Recording")
        while True:
            if self.abort_flag:
                input_stream.close()
                log.info("Abort Stream")
                self.state = RecordingState.DONE
                break

            if input_stream.closed:
                log.info("Stream Closed")
                self.state = RecordingState.DONE
                break

            data = b""
            for i in range(STREAMLINK_RETRY_COUNT):
                try:
                    data: bytes = input_stream.read(STREAMLINK_BUFFER_SIZE)
                    break
                except Exception as e:
                    print(f"HTTP Error: cnt={i} error={e}")

            if len(data) == 0:
                continue

            self.idx += 1
            file_path = f"{out_dir_path}/{self.idx}.ts"
            threading.Thread(target=self.ac.write, args=(file_path, data)).start()

        return out_dir_path
