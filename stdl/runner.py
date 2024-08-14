import logging
import time

from stdl.env import get_env
from stdl.logger import log
from stdl.StreamRecorder import StreamRecorder


def disable_streamlink_log():
    logging.getLogger("streamlink").setLevel(logging.CRITICAL)


def convert_time(secs: int) -> str:
    hours = secs // 3600
    remaining_seconds = secs % 3600
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60

    return f"{hours}:{minutes}:{seconds}"


def run():
    disable_streamlink_log()
    env = get_env()
    log.info("Request Env", env)
    recorder = StreamRecorder(env)
    recorder.observe()
    idx = 0
    while True:
        if idx % 10 == 0:
            log.info("Running App...", {
                "time": convert_time(idx),
                "state": recorder.state.name,
            })
        time.sleep(1)
        idx += 1

