import json
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from os.path import join
from typing import Optional

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType, FsType
from stdl.downloaders.streamlink.listener import RecorderListener
from stdl.downloaders.streamlink.stream import StreamlinkManager, StreamlinkArgs
from stdl.downloaders.streamlink.types import AbstractRecorder, RecordState
from stdl.event.done_message import DoneMessage, DoneStatus
from stdl.utils.file import write_file, delete_file
from stdl.utils.logger import log

default_restart_delay_sec = 3
default_chunk_threshold = 10

DONE_QUEUE_NAME = "stdl.done"


@dataclass
class RecorderArgs:
    out_dir_path: str
    platform_type: PlatformType


class StreamRecorder(AbstractRecorder):

    def __init__(self, sargs: StreamlinkArgs, rargs: RecorderArgs, pub: Amqp, sub: Amqp):
        super().__init__(uid=sargs.uid, platform_type=rargs.platform_type)
        self.uid = sargs.uid
        self.platform_type = rargs.platform_type

        self.incomplete_dir_path = join(rargs.out_dir_path, "incomplete")
        os.makedirs(self.incomplete_dir_path, exist_ok=True)
        self.lock_path = f"{self.incomplete_dir_path}/{sargs.uid}/lock.json"

        self.restart_delay_sec = default_restart_delay_sec
        self.chunk_threshold = default_chunk_threshold
        self.streamlink = StreamlinkManager(sargs, self.incomplete_dir_path)
        self.listener = RecorderListener(self, sub)
        self.pub = pub

        self.is_done = False
        self.cancel_flag = False
        self.should_convert = False

        self.record_thread: Optional[threading.Thread] = None
        self.amqp_thread: Optional[threading.Thread] = None

    def get_state(self) -> RecordState:
        return self.streamlink.state

    def cancel(self):
        log.info("Cancel Request")
        self.streamlink.abort_flag = True
        self.cancel_flag = True

    def finish(self):
        log.info("Finish Request")
        self.streamlink.abort_flag = True

    def record(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

        if self.__is_locked():
            log.info("Skip Record because Locked")
            return

        self.record_thread = threading.Thread(target=self.__record)
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

    def __record(self):
        try:
            self.__record_once()
            self.__close()
            log.info("Complete Recording", {"latest_state": self.streamlink.state.name})
        except:
            self.__unlock()
            self.__close()
            raise

    def __close(self):
        self.listener.close()
        self.amqp_thread.join()
        self.pub.close()
        self.is_done = True

    def __record_once(self):
        while True:
            self.__lock()
            streams = self.streamlink.wait_for_live()
            vid_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.streamlink.record(streams, vid_name)
            self.__unlock()

            # if self.cancel_flag:
            #     shutil.rmtree(chunks_path)
            #     self.clear_incomplete_dir()
            #     break

            # if self.should_convert:
            #     self.__convert_video(chunks_path)
            # else:
            #     self.__move_chunks(chunks_path)

            if self.cancel_flag:
                self.publish_done(DoneStatus.CANCELED, vid_name)
                # TODO: remove break
                break
            else:
                self.publish_done(DoneStatus.COMPLETE, vid_name)

            # TODO: remove codes below
            if self.__is_locked():
                break
            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break
            self.streamlink.abort_flag = False

    def publish_done(self, status: DoneStatus, vid_name: str):
        body = json.dumps(DoneMessage(
            status=status,
            ptype=self.platform_type,
            uid=self.uid,
            vidname=vid_name,
            fstype=FsType.LOCAL,
        ).model_dump(mode="json")).encode("utf-8")
        self.pub.connect()
        self.pub.assert_queue(DONE_QUEUE_NAME, auto_delete=False)
        self.pub.publish(DONE_QUEUE_NAME, body)
        self.pub.close()

    # def __move_chunks(self, chunks_path: str):
    #     shutil.move(chunks_path, chunks_path.replace(incomplete, complete))
    #     self.clear_incomplete_dir()
    #
    # def __convert_video(self, chunks_path: str):
    #     if len(os.listdir(chunks_path)) < self.chunk_threshold:
    #         # Remove chunks if not enough
    #         log.info("Skip Postprocess")
    #         shutil.rmtree(chunks_path)
    #     else:
    #         self.merge_hls_chunks(chunks_path)
    #         if os.path.exists(chunks_path):
    #             shutil.rmtree(chunks_path)
    #
    # def merge_hls_chunks(self, chunks_path: str):
    #     # merge ts files
    #     merged_ts_path = merge_ts(chunks_path)
    #     shutil.rmtree(chunks_path)
    #
    #     # convert ts to mp4
    #     incomplete_mp4_path = f"{chunks_path}.mp4"
    #     convert_vid(merged_ts_path, incomplete_mp4_path)
    #     os.remove(merged_ts_path)
    #
    #     # move mp4 file
    #     mp4_path = incomplete_mp4_path.replace(incomplete, complete)
    #     os.makedirs(join(self.complete_dir_path, self.uid), exist_ok=True)
    #     shutil.move(incomplete_mp4_path, mp4_path)
    #
    #     # clear incomplete directory
    #     self.clear_incomplete_dir()
    #
    #     log.info("Convert file", {"file_path": mp4_path})
    #     return mp4_path
    #
    # def clear_incomplete_dir(self):
    #     incomplete_name_dir_path = join(self.incomplete_dir_path, self.uid)
    #     if len(os.listdir(incomplete_name_dir_path)) == 0:
    #         os.rmdir(incomplete_name_dir_path)
    #     if len(os.listdir(self.incomplete_dir_path)) == 0:
    #         os.rmdir(self.incomplete_dir_path)

    def __lock(self):
        write_file(self.lock_path, "")

    def __unlock(self):
        delete_file(self.lock_path)
        log.info("Unlock")

    def __is_locked(self):
        return os.path.exists(self.lock_path)

    def __handle_sigterm(self, *acrgs):
        self.__unlock()
        self.__close()
