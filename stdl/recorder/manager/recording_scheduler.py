import time
from threading import Thread

from pyutils import log, error_dict

from .live_recorder import LiveRecorder
from ..manager.recorder_resolver import RecorderResolver
from ..schema.recording_constants import SCHEDULER_CHECK_DELAY_SEC
from ...common.env import Env
from ...common.request import AppRequest
from ...common.spec import PlatformType
from ...file import create_fs_writer


class RecordingScheduler:
    def __init__(self, env: Env):
        self.env = env
        self.__recorder_map: dict[str, LiveRecorder] = {}
        self.check_thread: Thread | None = None
        self.start_monitoring_states()

    def ger_status(self):
        return {
            # "threads": [{"id": th.ident, "name": th.name} for th in threading.enumerate()],
            "recorders": [recorder.get_state() for recorder in self.__recorder_map.values()],
        }

    def record(self, req: AppRequest):
        writer = create_fs_writer(self.env)
        recorder = RecorderResolver(self.env, req, writer).create_recorder()
        key = create_key(recorder.platform, recorder.channel_id)
        if self.__recorder_map.get(key):
            log.info("Already Recording")
            return
        self.__recorder_map[key] = recorder
        recorder.record(block=False)

    def cancel(self, platform_type: PlatformType, uid: str):
        key = create_key(platform_type, uid)
        if self.__recorder_map.get(key):
            self.__recorder_map[key].stream.state.cancel()
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


def create_key(platform_type: PlatformType, uid: str) -> str:
    return f"{platform_type.value}:{uid}"
