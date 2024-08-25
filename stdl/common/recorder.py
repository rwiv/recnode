import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import streamlink
from streamlink.options import Options
from streamlink.stream import HLSStream
from streamlink.stream.hls import HLSStreamReader
from stdl.utils.logger import log, get_error_info


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


@dataclass
class StreamRecorderArgs:
    url: str
    name: str
    out_dir: str
    cookies: Optional[str] = None
    options: Optional[dict[str, str]] = None
    wait_interval: int = 1
    restart_delay: int = 40


class StreamRecorder:

    def __init__(self, args: StreamRecorderArgs):
        self.name = args.name
        self.out_dir = args.out_dir
        self.cookies = args.cookies
        self.options = args.options
        self.url = args.url
        self.delay_sec = args.wait_interval
        self.restart_delay = args.restart_delay

        self.is_done = False
        self.thread: threading.Thread | None = None
        self.state: RecordState = RecordState.WAIT

    def stop(self):
        self.is_done = True
        self.thread.join()

    def get_session(self) -> streamlink.session.Streamlink:
        session = streamlink.session.Streamlink()
        if self.cookies is not None:
            data: list[dict] = json.loads(self.cookies)
            for cookie in data:
                session.http.cookies.set(cookie["name"], cookie["value"])
        return session

    def __wait_for_live(self) -> dict[str, HLSStream]:
        cnt = 0
        while True:
            self.state = RecordState.WAIT
            try:
                session = self.get_session()
                if self.options is not None:
                    options = Options()
                    for key, value in self.options.items():
                        options.set(key, value)
                    streams: dict[str, HLSStream] = session.streams(self.url, options=options)
                else:
                    streams: dict[str, HLSStream] = session.streams(self.url)

                if streams != {}:
                    log.info("Stream Start")
                    return streams
            except (Exception,):
                log.error(*get_error_info())

            time.sleep(self.delay_sec)
            cnt += 1
            if cnt >= 10:
                log.info("Wait For Live", {"name": self.name})
                cnt = 0

    def observe(self):
        self.thread = threading.Thread(target=self.__observe)
        self.thread.daemon = True
        self.thread.start()

    def __observe(self):
        while True:
            self.__record()
            time.sleep(self.restart_delay)
            log.info("Restart Record", {"latest_state": self.state.name})

    def __record(self):
        streams = self.__wait_for_live()
        if streams == {}:
            self.state = RecordState.FAILED
            return

        formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_path = f"{self.out_dir}/{self.name}/{formatted_time}"

        stream = streams["best"]
        input_stream: HLSStreamReader = stream.open()
        self.state = RecordState.RECORDING

        idx = 0

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        log.info("Start recording")
        while True:
            if self.is_done:
                log.info("Recording Stop")
                self.state = RecordState.DONE
                break
            if input_stream.closed:
                log.info("Stream closed")
                self.state = RecordState.DONE
                break

            data: bytes = input_stream.read(sys.maxsize)

            if len(data) > 0:
                with open(f"{dir_path}/{idx}.ts", "wb") as f:
                    log.info("Write .ts file", {
                        "name": self.name,
                        "idx": idx,
                        "size": len(data),
                    })
                    f.write(data)
                idx += 1
