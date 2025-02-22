import threading
import time
from threading import Thread

from pyutils import stacktrace_entry, log

from .recorder import StreamRecorder
from ..platform.recorder_resolver import RecorderResolver
from ..spec.recording_constants import SCHEDULER_CHECK_DELAY_SEC
from ...common.env import Env
from ...common.fs import create_fs_writer
from ...common.request import AppRequest
from ...common.spec import PlatformType


class RecordingScheduler:
    def __init__(self, env: Env):
        self.env = env
        self.__recorder_map: dict[str, StreamRecorder] = {}
        self.check_thread: Thread | None = None
        self.start_monitoring_states()

    def ger_status(self):
        return {
            "threads": [{"id": th.ident, "name": th.name} for th in threading.enumerate()],
            "recorders": [recorder.get_state() for recorder in self.__recorder_map.values()],
        }

    def record(self, req: AppRequest):
        writer = create_fs_writer(self.env.fs_type, self.env.fs_name, self.env.fs_config_path)
        recorder = RecorderResolver(self.env, req, writer).create_recorder()
        key = create_key(recorder.platform_type, recorder.uid)
        if self.__recorder_map.get(key):
            log.info("Already Recording")
            return
        self.__recorder_map[key] = recorder
        recorder.record(block=False)

    def cancel(self, platform_type: PlatformType, uid: str):
        key = create_key(platform_type, uid)
        if self.__recorder_map.get(key):
            self.__recorder_map[key].cancel()
        else:
            log.info(f"Not found recorder: platform={platform_type}, uid={uid}")

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
                        if recorder.record_thread:
                            recorder.record_thread.join()
                        if recorder.amqp_thread:
                            recorder.amqp_thread.join()
                        log.info(
                            f"Remove Done Recorder: platform={recorder.platform_type}, uid={recorder.uid}"
                        )
                        del self.__recorder_map[key]
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)
            except:
                log.error(*stacktrace_entry())
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)


def create_key(platform_type: PlatformType, uid: str) -> str:
    return f"{platform_type.value}:{uid}"
