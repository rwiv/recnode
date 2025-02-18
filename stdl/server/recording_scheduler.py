import time
from threading import Thread

from stdl.common.amqp import AmqpHelperMock, AmqpHelperBlocking
from stdl.common.env import Env
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.platforms.chzzk.recorder import ChzzkLiveRecorder
from stdl.utils.error import stacktrace
from stdl.utils.fs.fs_common_abstract import FsAccessor
from stdl.utils.fs.fs_local import LocalFsAccessor
from stdl.utils.logger import log


class RecordingScheduler:
    def __init__(self, env: Env):
        self.env = env
        self.__recorder_map: dict[str, StreamRecorder] = {}
        self.check_thread: Thread | None = None
        self.check()

    def get_recording_count(self) -> int:
        return len(self.__recorder_map)

    def record(self, uid: str):
        recorder = ChzzkLiveRecorder(
            uid,
            self.env.out_dir_path,
            None,
            self.__create_accessor(),
            self.__create_amqp(),
            self.__create_amqp(),
        )
        if self.__recorder_map.get(uid):
            log.info("Already Recording")
            return
        self.__recorder_map[uid] = recorder
        recorder.record(False)

    def cancel(self, uid: str):
        if self.__recorder_map.get(uid):
            self.__recorder_map[uid].cancel()
        else:
            log.info(f"Not found recorder: uid={uid}")

    def check(self):
        self.check_thread = Thread(target=self.__check)
        self.check_thread.daemon = True
        self.check_thread.start()

    def __check(self):
        while True:
            try:
                keys = list(self.__recorder_map.keys())
                for uid in keys:
                    recorder = self.__recorder_map.get(uid)
                    if recorder is not None and recorder.is_done:
                        log.info(f"Remove Done Recorder: uid={uid}")
                        del self.__recorder_map[uid]
                time.sleep(1)
            except:
                print(stacktrace())
                time.sleep(1)

    def __create_accessor(self) -> FsAccessor:
        return LocalFsAccessor()

    def __create_amqp(self):
        # return AmqpBlocking(self.env.amqp)
        if self.env.env == "prod":
            return AmqpHelperBlocking(self.env.amqp)
        else:
            return AmqpHelperMock()
