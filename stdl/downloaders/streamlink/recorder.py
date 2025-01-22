import os
import shutil
import signal
import threading
import time
from os.path import dirname

from stdl.common.amqp import Amqp
from stdl.downloaders.hls.merge import merge_hls_chunks
from stdl.downloaders.streamlink.interface import IRecorder
from stdl.downloaders.streamlink.listener import Listener
from stdl.downloaders.streamlink.stream import StreamlinkManager, StreamlinkArgs
from stdl.utils.file import write_file, delete_file
from stdl.utils.logger import log

default_restart_delay_sec = 3
default_chunk_threshold = 10


class StreamRecorder(IRecorder):

    def __init__(self, args: StreamlinkArgs, once: bool, amqp: Amqp):
        self.name = args.name
        self.once = once
        self.out_dir_path = args.out_dir_path
        self.tmp_dir_path = args.tmp_dir_path

        self.lock_path = f"{args.out_dir_path}/{args.name}/lock.json"
        self.restart_delay_sec = default_restart_delay_sec
        self.chunk_threshold = default_chunk_threshold
        self.streamlink = StreamlinkManager(args)
        self.listener = Listener(self, amqp)

        self.is_done = False
        self.cancel_flag = False
        self.finish_flag = False

    def get_name(self) -> str:
        return self.name

    def close(self):
        self.listener.close()
        self.is_done = True

    def record(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

        record_thread = threading.Thread(target=self._record)
        record_thread.daemon = True
        record_thread.start()

        amqp_thread = threading.Thread(target=self.listener.consume)
        amqp_thread.daemon = True
        amqp_thread.start()

        while True:
            if self.is_done:
                break
            time.sleep(1)

    def cancel(self):
        log.info("Cancel")
        self.streamlink.abort_flag = True
        self.cancel_flag = True
        self.finish_flag = True  # postprocess 도중 종료되는 경우를 대비

    def finish(self):
        log.info("Finish")
        self.streamlink.abort_flag = True
        self.finish_flag = True

    def _record(self):
        try:
            if self.__is_locked():
                log.info("Skip Record because Locked")
                self.close()
                return

            if self.once:
                self.__record_once()
                self.close()
            else:
                self.__record_endless()
            log.info("End Record", {"latest_state": self.streamlink.state.name})
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
            log.info("Restart Record", {"latest_state": self.streamlink.state.name})
        self.__unlock()

    def __record(self) -> str:
        streams = self.streamlink.wait_for_live()
        log.info("Stream Start")
        return self.streamlink.record(streams)

    def __postprocess(self, chunks_path: str):
        if len(os.listdir(chunks_path)) < self.chunk_threshold:
            # Remove chunks if not enough
            log.info("Skip postprocess chunks")
            shutil.rmtree(chunks_path)
        else:
            # move to tmp dir
            tmp_chunks_path = chunks_path.replace(self.out_dir_path, self.tmp_dir_path)
            if os.path.exists(tmp_chunks_path) is False:
                shutil.copytree(chunks_path, tmp_chunks_path)
            merge_hls_chunks(tmp_chunks_path, self.out_dir_path, self.name)
            if os.path.exists(chunks_path):
                shutil.rmtree(chunks_path)

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
