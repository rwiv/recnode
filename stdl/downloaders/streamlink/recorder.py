import os
import shutil
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
            tmp_dir_path: str,
            once: bool = True,
    ):
        self.name = args.name
        self.once = once
        self.tmp_dir_path = tmp_dir_path

        self.channel_path = f"{args.out_dir_path}/{args.name}"
        self.lock_path = f"{self.channel_path}/lock.json"
        self.restart_delay_sec = 3
        self.chunk_threshold = 20
        self.streamlink = StreamlinkManager(StreamlinkArgs(
            url=args.url,
            name=args.name,
            out_dir_path=args.out_dir_path,
            cookies=args.cookies,
            options=args.options,
        ))

    def record(self):
        # if os.path.exists(self.lock_path):
        #     log.info("Skip Record")
        #     return

        if self.once:
            self.__record_once()
        else:
            self.__record_endless()

    def __record_once(self):
        # write_file(self.lock_path, "")

        while True:
            thread = self.__record()
            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break

        # delete_file(self.lock_path)
        log.info("End Record", {"latest_state": self.streamlink.state.name})
        thread.join()

    def __record_endless(self):
        # write_file(self.lock_path, "")

        while True:
            self.__record()
            time.sleep(self.restart_delay_sec)
            log.info("Restart Record", {"latest_state": self.streamlink.state.name})

    def __record(self):
        streams = self.streamlink.wait_for_live()
        log.info("Stream Start")

        chunks_path = self.streamlink.record(streams)

        thread = None
        if len(os.listdir(chunks_path)) < self.chunk_threshold:
            shutil.rmtree(chunks_path)
        else:
            thread = threading.Thread(
                target=merge_chunks,
                args=(chunks_path, self.tmp_dir_path, self.name),
            )
            thread.daemon = True
            thread.start()
        return thread
