import os
import shutil
import signal
import threading
import time
from dataclasses import dataclass
from os.path import dirname, join
from typing import Optional

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.hls.merge import merge_ts, convert_vid
from stdl.downloaders.streamlink.types import IRecorder, RecordState
from stdl.downloaders.streamlink.listener import Listener
from stdl.downloaders.streamlink.stream import StreamlinkManager, StreamlinkArgs
from stdl.utils.file import write_file, delete_file
from stdl.utils.logger import log

default_restart_delay_sec = 3
default_chunk_threshold = 10

incomplete = "incomplete"
complete = "complete"


@dataclass
class RecorderArgs:
    out_dir_path: str
    platform_type: PlatformType
    once: bool


class StreamRecorder(IRecorder):

    def __init__(self, sargs: StreamlinkArgs, rargs: RecorderArgs, amqp: Amqp):
        self.uid = sargs.uid
        self.once = rargs.once
        self.platform_type = rargs.platform_type

        self.complete_dir_path = join(rargs.out_dir_path, complete)
        self.incomplete_dir_path = join(rargs.out_dir_path, incomplete)
        os.makedirs(self.incomplete_dir_path, exist_ok=True)
        self.lock_path = f"{self.incomplete_dir_path}/{sargs.uid}/lock.json"

        self.restart_delay_sec = default_restart_delay_sec
        self.chunk_threshold = default_chunk_threshold
        self.streamlink = StreamlinkManager(sargs, self.incomplete_dir_path)
        self.listener = Listener(self, amqp)

        self.is_done = False
        self.cancel_flag = False
        self.finish_flag = False

        self.record_thread: Optional[threading.Thread] = None
        self.amqp_thread: Optional[threading.Thread] = None

    def get_uid(self) -> str:
        return self.uid

    def get_state(self) -> RecordState:
        return self.streamlink.state

    def get_platform_type(self) -> PlatformType:
        return self.platform_type

    def close(self):
        self.listener.close()
        self.amqp_thread.join()
        self.is_done = True

    def record(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

        if self.__is_locked():
            log.info("Skip Record because Locked")
            return

        self.record_thread = threading.Thread(target=self._record)
        self.record_thread.daemon = True
        self.record_thread.start()

        self.amqp_thread = threading.Thread(target=self.listener.consume)
        self.amqp_thread.daemon = True
        self.amqp_thread.start()

        while True:
            if self.is_done:
                self.record_thread.join()
                log.info("Done")
                break
            time.sleep(1)

    def cancel(self):
        log.info("Cancel Request")
        self.streamlink.abort_flag = True
        self.cancel_flag = True
        self.finish_flag = True  # postprocess 도중 종료되는 경우를 대비

    def finish(self):
        log.info("Finish Request")
        self.streamlink.abort_flag = True
        self.finish_flag = True

    def _record(self):
        try:
            if self.once:
                self.__record_once()
                self.close()
            else:
                self.__record_endless()
            log.info("Complete Recording", {"latest_state": self.streamlink.state.name})
        except:
            self.__unlock()
            self.close()
            raise

    def __record_once(self):
        while True:
            self.__lock()
            chunks_path = self.__record()
            self.__unlock()

            if self.cancel_flag:
                clear_chunks_path(chunks_path)
                break
            self.__postprocess(chunks_path)
            if self.finish_flag or self.__is_locked():
                break

            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break

    def __record_endless(self):
        self.__lock()
        while True:
            chunks_path = self.__record()
            if self.cancel_flag:
                clear_chunks_path(chunks_path)
                break
            self.__postprocess(chunks_path)
            if self.finish_flag:
                break
            time.sleep(self.restart_delay_sec)
            log.info("Restart Recording", {"latest_state": self.streamlink.state.name})
        self.__unlock()

    def __record(self) -> str:
        streams = self.streamlink.wait_for_live()
        return self.streamlink.record(streams)

    def __postprocess(self, chunks_path: str):
        if len(os.listdir(chunks_path)) < self.chunk_threshold:
            # Remove chunks if not enough
            log.info("Skip Postprocess")
            shutil.rmtree(chunks_path)
        else:
            self.merge_hls_chunks(chunks_path)
            if os.path.exists(chunks_path):
                shutil.rmtree(chunks_path)

    def merge_hls_chunks(self, chunks_path: str):
        # merge ts files
        merged_ts_path = merge_ts(chunks_path)
        shutil.rmtree(chunks_path)

        # convert ts to mp4
        os.makedirs(join(self.complete_dir_path, self.uid), exist_ok=True)
        mp4_path = f"{chunks_path}.mp4".replace(incomplete, complete)
        convert_vid(merged_ts_path, mp4_path)
        os.remove(merged_ts_path)

        incomplete_name_dir_path = join(self.incomplete_dir_path, self.uid)
        if len(os.listdir(incomplete_name_dir_path)) == 0:
            os.rmdir(incomplete_name_dir_path)
        if len(os.listdir(self.incomplete_dir_path)) == 0:
            os.rmdir(self.incomplete_dir_path)

        log.info("Convert file", {"file_path": mp4_path})
        return mp4_path

    def __lock(self):
        write_file(self.lock_path, "")

    def __unlock(self):
        delete_file(self.lock_path)
        log.info("Unlock")

    def __is_locked(self):
        return os.path.exists(self.lock_path)

    def __handle_sigterm(self, *acrgs):
        self.__unlock()
        self.close()


def clear_chunks_path(chunks_path):
    shutil.rmtree(chunks_path)
    dir_path = dirname(chunks_path)
    if len(os.listdir(dir_path)) == 0:
        os.rmdir(dir_path)
