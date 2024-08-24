import json
import os
import sys
import threading
import time
from datetime import datetime
from enum import Enum
import streamlink
from streamlink.stream import HLSStream
from streamlink.stream.hls import HLSStreamReader
from stdl.utils.logger import log, get_error_info


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


class StreamRecorder:

    def __init__(
            self,
            uid: str,
            out_dir: str,
            cookies: str,
            wait_interval: int = 1,
            restart_delay: int = 40,
    ):
        self.uid = uid
        self.out_dir = out_dir
        self.cookies = cookies
        self.url = f"https://chzzk.naver.com/live/{self.uid}"
        self.delay_sec = wait_interval
        self.is_done = False
        self.thread: threading.Thread | None = None
        self.state: RecordState = RecordState.WAIT
        self.restart_delay = restart_delay

    def stop(self):
        self.is_done = True
        self.thread.join()

    def __get_session(self) -> streamlink.session.Streamlink:
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
                session = self.__get_session()
                streams: dict[str, HLSStream] = session.streams(self.url)
                if streams != {}:
                    log.info("Stream Start")
                    return streams
            except (Exception,):
                log.error(*get_error_info())

            time.sleep(self.delay_sec)
            cnt += 1
            if cnt >= 10:
                log.info("Wait For Live", {"uid": self.uid})
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
        dir_path = f"{self.out_dir}/{self.uid}/{formatted_time}"

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
                        "uid": self.uid,
                        "idx": idx,
                        "size": len(data),
                    })
                    f.write(data)
                idx += 1
