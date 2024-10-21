import os
import shutil
import signal
import threading
import time

from stdl.downloaders.streamlink.merge import merge_chunks
from stdl.downloaders.streamlink.stream import StreamlinkManager, StreamlinkArgs
from stdl.utils.file import write_file, delete_file
from stdl.utils.logger import log


class StreamRecorder:

    def __init__(
            self,
            args: StreamlinkArgs,
            once: bool = True,
    ):
        self.name = args.name
        self.once = once
        self.out_dir_path = args.out_dir_path
        self.tmp_dir_path = args.tmp_dir_path

        self.lock_path = f"{args.out_dir_path}/{args.name}/lock.json"
        self.restart_delay_sec = 3
        self.chunk_threshold = 20
        self.streamlink = StreamlinkManager(args)

    def record(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm)
        try:
            if self.__is_locked():
                log.info("Skip Record because Locked")
                return

            if self.once:
                self.__record_once()
            else:
                self.__record_endless()

            self.__unlock()
        except:
            self.__unlock()
            log.info("Unlock")
            raise

    def __record_once(self):
        while True:
            self.__lock()

            tmp_chunks_path = self.__record()
            thread = self.__postprocess_async(tmp_chunks_path)

            self.__unlock()
            if thread is not None:
                thread.join()

            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break

        log.info("End Record", {"latest_state": self.streamlink.state.name})

    def __record_endless(self):
        self.__lock()

        while True:
            tmp_chunks_path = self.__record()
            self.__postprocess_async(tmp_chunks_path)
            time.sleep(self.restart_delay_sec)
            log.info("Restart Record", {"latest_state": self.streamlink.state.name})

    def __record(self) -> str:
        streams = self.streamlink.wait_for_live()
        log.info("Stream Start")

        return self.streamlink.record(streams)

    def __postprocess_async(self, tmp_chunks_path: str):
        thread = None
        if len(os.listdir(tmp_chunks_path)) < self.chunk_threshold:
            # Remove chunks if not enough
            shutil.rmtree(tmp_chunks_path)
        else:
            # Merge chunks
            thread = threading.Thread(
                target=merge_chunks,
                args=(tmp_chunks_path, self.out_dir_path, self.name),
            )
            thread.daemon = True
            thread.start()
        return thread

    def __lock(self):
        write_file(self.lock_path, "")

    def __unlock(self):
        delete_file(self.lock_path)

    def __is_locked(self):
        return os.path.exists(self.lock_path)

    def __handle_sigterm(self, *acrgs):
        self.__unlock()
