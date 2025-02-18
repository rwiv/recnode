import threading
import time
from datetime import datetime
from threading import Thread

from stdl.common.amqp import AmqpHelper
from stdl.common.types import FsType
from stdl.record.recorder.listener import RecorderListener, EXIT_QUEUE_PREFIX
from stdl.record.recorder.recorder_abc import AbstractRecorder
from stdl.record.recorder.streamlink import StreamlinkManager
from stdl.record.spec.recording_arguments import StreamlinkArgs, RecorderArgs
from stdl.record.spec.done_message import DoneMessage, DoneStatus
from stdl.record.spec.recording_status import RecorderStatus
from stdl.utils.fs.fs_common_abstract import FsAccessor
from stdl.utils.fs.fs_local import LocalFsAccessor
from stdl.utils.logger import log
from stdl.utils.path import path_join

default_restart_delay_sec = 3
default_chunk_threshold = 10

DONE_QUEUE_NAME = "stdl.done"


class StreamRecorder(AbstractRecorder):

    def __init__(
        self,
        stream_args: StreamlinkArgs,
        recorder_args: RecorderArgs,
        fs_accessor: FsAccessor,
        amqp_helper: AmqpHelper,
    ):
        super().__init__(uid=stream_args.uid, platform_type=recorder_args.platform_type)
        self.ac = fs_accessor
        self.uid = stream_args.uid
        self.url = stream_args.url
        self.platform_type = recorder_args.platform_type
        self.use_credentials = recorder_args.use_credentials

        self.vid_name: str | None = None
        self.incomplete_dir_path = "incomplete"
        if isinstance(self.ac, LocalFsAccessor):
            self.incomplete_dir_path = path_join(recorder_args.out_dir_path, "incomplete")
        self.ac.mkdir(self.incomplete_dir_path)
        self.lock_path = f"{self.incomplete_dir_path}/{stream_args.uid}/lock.json"

        self.restart_delay_sec = default_restart_delay_sec
        self.chunk_threshold = default_chunk_threshold
        self.streamlink = StreamlinkManager(stream_args, self.incomplete_dir_path, self.ac)
        self.listener = RecorderListener(self, amqp_helper)
        self.amqp = amqp_helper

        self.is_done = False
        self.cancel_flag = False

        self.record_thread: Thread | None = None
        self.amqp_thread: Thread | None = None

    def get_state(self):
        return RecorderStatus(
            platform=self.platform_type,
            uid=self.uid,
            idx=self.streamlink.idx,
            streamStatus=self.streamlink.state,
        )

    def cancel(self):
        log.info("Cancel Request")
        self.streamlink.abort_flag = True
        self.cancel_flag = True

    def finish(self):
        log.info("Finish Request")
        self.streamlink.abort_flag = True

    def record(self, block: bool = True):
        log.info(f"Start Record: {self.url}")
        if self.use_credentials:
            log.info("Using Credentials")

        self.record_thread = threading.Thread(target=self.__record)
        self.record_thread.start()

        if block is False:
            return

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
            self.__close()
            raise

    def __close(self):
        conn = self.listener.conn
        if conn is not None:

            def close_conn():
                self.listener.amqp.close(conn)

            conn.add_callback_threadsafe(close_conn)
        if self.amqp_thread is not None:
            self.amqp_thread.join()
        self.is_done = True

    def __record_once(self):
        # Todo: remove while loop
        while True:
            streams = self.streamlink.wait_for_live()
            # abort_flag is set by cancel method
            if streams is None:
                break

            if self.__is_recording_active():
                log.info("Recording is already in progress, skipping recording")
                break

            # Start AMQP consumer thread
            self.amqp_thread = threading.Thread(target=self.listener.consume)
            self.amqp_thread.daemon = True  # AMQP connection should be released on abnormal termination
            self.amqp_thread.start()

            # Start recording
            vid_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.vid_name = vid_name
            self.streamlink.record(streams, vid_name)

            if self.cancel_flag:
                self.__publish_done(DoneStatus.CANCELED, vid_name)
                # TODO: remove break
                break
            else:
                self.__publish_done(DoneStatus.COMPLETE, vid_name)

            # TODO: remove codes below
            time.sleep(self.restart_delay_sec)
            if self.streamlink.get_streams() == {}:
                break
            self.streamlink.abort_flag = False

    def __is_recording_active(self):
        vid_queue_name = f"{EXIT_QUEUE_PREFIX}.{self.platform_type.value}.{self.uid}"
        conn, chan = self.amqp.connect()
        exists = self.amqp.queue_exists(chan, vid_queue_name)
        self.amqp.close(conn)
        return exists

    def __publish_done(self, status: DoneStatus, vid_name: str):
        msg = DoneMessage(
            status=status,
            ptype=self.platform_type,
            uid=self.uid,
            vidname=vid_name,
            fstype=FsType.LOCAL,
        ).model_dump_json(by_alias=True)
        conn, chan = self.amqp.connect()
        self.amqp.ensure_queue(chan, DONE_QUEUE_NAME, auto_delete=False)
        self.amqp.publish(chan, DONE_QUEUE_NAME, msg.encode("utf-8"))
        self.amqp.close(conn)
        log.info("Published Done Message")
