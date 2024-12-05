import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import streamlink
from streamlink.options import Options
from streamlink.stream import HLSStream
from streamlink.stream.hls import HLSStreamReader

from stdl.utils.file import write_bfile
from stdl.utils.logger import log, get_error_info

retry_count = 5
buf_size = sys.maxsize
# buf_size = 4 * 1024 * 1024


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


@dataclass
class StreamlinkArgs:
    url: str
    name: str
    out_dir_path: str
    tmp_dir_path: str
    cookies: Optional[str] = None
    options: Optional[dict[str, str]] = None


class StreamlinkManager:

    def __init__(self, args: StreamlinkArgs):
        self.url = args.url
        self.name = args.name
        self.out_dir_path = args.out_dir_path
        self.cookies = args.cookies
        self.options = args.options

        self.wait_delay_sec = 1
        self.state: RecordState = RecordState.WAIT

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

    def wait_for_live(self) -> dict[str, HLSStream]:
        log.info("Wait For Live")
        while True:
            self.state = RecordState.WAIT
            try:
                streams = self.get_streams()
                if streams != {}:
                    return streams
            except (Exception,):
                log.error(*get_error_info())

            time.sleep(self.wait_delay_sec)

    def record(self, streams: dict[str, HLSStream]) -> str:
        formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir_path = f"{self.out_dir_path}/{self.name}/{formatted_time}"

        input_stream: HLSStreamReader = streams["best"].open()
        self.state = RecordState.RECORDING

        idx = 0

        if not os.path.exists(out_dir_path):
            os.makedirs(out_dir_path)

        log.info("Start recording")
        while True:
            if input_stream.closed:
                log.info("Stream closed")
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
