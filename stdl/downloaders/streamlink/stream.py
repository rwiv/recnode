import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

import streamlink
from streamlink.options import Options
from streamlink.stream import HLSStream
from streamlink.stream.hls import HLSStreamReader

from stdl.downloaders.streamlink.types import RecordState
from stdl.utils.error import stacktrace
from stdl.utils.file import write_bfile
from stdl.utils.logger import log

retry_count = 5
buf_size = sys.maxsize
# buf_size = 4 * 1024 * 1024


@dataclass
class StreamlinkArgs:
    url: str
    uid: str
    cookies: Optional[str] = None
    options: Optional[dict[str, str]] = None


class StreamlinkManager:

    def __init__(self, args: StreamlinkArgs, out_dir_path: str):
        self.url = args.url
        self.uid = args.uid
        self.out_dir_path = out_dir_path
        self.cookies = args.cookies
        self.options = args.options

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

    def get_session(self) -> streamlink.session.Streamlink:
        session = streamlink.session.Streamlink()
        if self.cookies is not None:
            data: list[dict] = json.loads(self.cookies)
            for cookie in data:
                session.http.cookies.set(cookie["name"], cookie["value"])
        return session

    def wait_for_live(self) -> Optional[dict[str, HLSStream]]:
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

        if not os.path.exists(out_dir_path):
            os.makedirs(out_dir_path)

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
            write_bfile(f"{out_dir_path}/{idx}.ts", data, False)

        return out_dir_path
