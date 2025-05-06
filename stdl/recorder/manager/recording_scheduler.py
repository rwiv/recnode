import threading
import time
from threading import Thread

from pyutils import log, error_dict

from .live_recorder import LiveRecorder
from ..manager.recorder_resolver import RecorderResolver
from ..schema.recording_constants import SCHEDULER_CHECK_DELAY_SEC
from ...common.env import Env
from ...data.live import LiveState
from ...file import create_fs_writer


class RecordingScheduler:
    def __init__(self, env: Env):
        self.env = env
        self.__recorder_map: dict[str, LiveRecorder] = {}
        self.check_thread: Thread | None = None
        self.start_monitoring_states()

    def ger_status(self, full_stats: bool = False):
        recorders = [recorder.get_status(full_stats=full_stats) for recorder in self.__recorder_map.values()]
        return {
            # "threads": [{"id": th.ident, "name": th.name} for th in threading.enumerate()],
            "thread_cnt": len(threading.enumerate()),
            "recorders": recorders,
        }

    def record(self, state: LiveState):
        writer = create_fs_writer(self.env)
        recorder = RecorderResolver(self.env, writer).create_recorder(state)

        key = parse_key(state)
        if self.__recorder_map.get(key):
            log.info("Already Recording")
            return
        self.__recorder_map[key] = recorder
        recorder.record(state=state, block=False)

    def cancel(self, state: LiveState):
        recorder = self.__recorder_map.get(parse_key(state))
        if recorder is not None:
            recorder.stream.state.cancel()
        else:
            log.error(f"Not found recorder", state.model_dump(mode="json"))

    def start_monitoring_states(self):
        self.check_thread = Thread(target=self.__monitor_states)
        self.check_thread.daemon = True
        self.check_thread.start()

    def __monitor_states(self):
        while True:
            try:
                keys = list(self.__recorder_map.keys())
                for key in keys:
                    recorder = self.__recorder_map.get(key)
                    if recorder is not None and recorder.is_done:
                        if recorder.recording_thread:
                            recorder.recording_thread.join()
                        log.info(
                            f"Remove Done Recorder: platform={recorder.platform}, uid={recorder.channel_id}"
                        )
                        del self.__recorder_map[key]
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)
            except Exception as e:
                log.error("Failed to monitor states", error_dict(e))
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)


def parse_key(state: LiveState) -> str:
    return f"{state.platform.value}:{state.channel_id}:{state.video_name}"
