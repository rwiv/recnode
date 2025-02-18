import time
from threading import Thread

from stdl.app.recorder_resolver import RecorderResolver
from stdl.common.env import Env
from stdl.common.fs_config_utils import create_fs_accessor
from stdl.common.request_config import AppConfig
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.utils.error import stacktrace
from stdl.utils.logger import log

CHECK_DELAY = 1


class RecordingScheduler:
    def __init__(self, env: Env):
        self.env = env
        self.__recorder_map: dict[str, StreamRecorder] = {}
        self.check_thread: Thread | None = None
        self.start_monitoring_states()

    def get_recording_count(self) -> int:
        return len(self.__recorder_map)

    def ger_status(self):
        return [recorder.get_state() for recorder in self.__recorder_map.values()]

    def record(self, req: AppConfig):
        ac = create_fs_accessor(self.env, req)
        ac.mkdir(self.env.out_dir_path)
        recorder = RecorderResolver(self.env, req, ac).create_recorder()
        key = create_key(recorder.platform_type, recorder.uid)
        if self.__recorder_map.get(key):
            log.info("Already Recording")
            return
        self.__recorder_map[key] = recorder
        recorder.record(False)

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
                time.sleep(CHECK_DELAY)
            except:
                print(stacktrace())
                time.sleep(CHECK_DELAY)


def create_key(platform_type: PlatformType, uid: str) -> str:
    return f"{platform_type.value}:{uid}"
