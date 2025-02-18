import json
import sys
import threading
import time

from pydantic import BaseModel, Field
from streamlink.options import Options
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream, HLSStreamReader

from stdl.downloaders.streamlink.types import RecordState
from stdl.utils.error import stacktrace
from stdl.utils.fs.fs_common_abstract import FsAccessor
from stdl.utils.logger import log

retry_count = 5
buf_size = sys.maxsize
# buf_size = 4 * 1024 * 1024


class StreamlinkArgs(BaseModel):
    url: str = Field(min_length=1)
    uid: str = Field(min_length=1)
    cookies: str | None = Field(min_length=1, default=None)
    options: dict[str, str] | None = Field(min_length=1, default=None)


class StreamlinkManager:

    def __init__(self, args: StreamlinkArgs, out_dir_path: str, ac: FsAccessor):
        self.url = args.url
        self.uid = args.uid
        self.out_dir_path = out_dir_path
        self.cookies = args.cookies
        self.options = args.options
        self.ac = ac

        self.wait_delay_sec = 1
        self.state: RecordState = RecordState.WAIT
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
                log.error("Failed to get streams")
                print(stacktrace())

            if cnt == 0:
                log.info("Wait For Live")
            self.state = RecordState.WAIT
            time.sleep(self.wait_delay_sec)
            cnt += 1

    def record(self, streams: dict[str, HLSStream], vid_name: str) -> str:
        out_dir_path = f"{self.out_dir_path}/{self.uid}/{vid_name}"

        input_stream: HLSStreamReader = streams["best"].open()
        self.state = RecordState.RECORDING

        idx = 0

        if not self.ac.exists(out_dir_path):
            self.ac.mkdir(out_dir_path)

        log.info("Start Recording")
        while True:
            if self.abort_flag:
                input_stream.close()
                log.info("Abort Stream")
                self.state = RecordState.DONE
                break

            if input_stream.closed:
                log.info("Stream Closed")
                self.state = RecordState.DONE
                break

            data = b""
            for i in range(retry_count):
                try:
                    data: bytes = input_stream.read(buf_size)
                    break
                except Exception as e:
                    print(f"HTTP Error: cnt={i} error={e}")

            if len(data) == 0:
                continue

            idx += 1
            file_path = f"{out_dir_path}/{idx}.ts"
            threading.Thread(target=self.ac.write, args=(file_path, data)).start()

        return out_dir_path
