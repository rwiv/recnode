import signal
import threading
import time
from datetime import datetime
from threading import Thread

from pyutils import log, path_join

from .listener import RecorderListener, EXIT_QUEUE_PREFIX
from .recorder_abc import AbstractRecorder
from .streamlink import StreamlinkManager
from ..spec.done_message import DoneStatus, DoneMessage
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ..spec.recording_constants import DONE_QUEUE_NAME, RECORDING_SHUTDOWN_SEC
from ..spec.recording_status import RecorderStatus
from ...common.amqp import AmqpHelper
from ...common.env import Env
from ...common.fs import ObjectWriter


class StreamRecorder(AbstractRecorder):

    def __init__(
        self,
        env: Env,
        stream_args: StreamlinkArgs,
        recorder_args: RecorderArgs,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        super().__init__(uid=stream_args.uid, platform_type=recorder_args.platform_type)
        self.env = env
        self.writer = writer
        self.uid = stream_args.uid
        self.url = stream_args.url
        self.platform_type = recorder_args.platform_type
        self.use_credentials = recorder_args.use_credentials

        self.vid_name: str | None = None
        self.incomplete_dir_path = self.writer.normalize_base_path(
            path_join(recorder_args.out_dir_path, "incomplete")
        )
        self.lock_path = f"{self.incomplete_dir_path}/{stream_args.uid}/lock.json"

        self.streamlink = StreamlinkManager(
            stream_args,
            recorder_args,
            self.incomplete_dir_path,
            self.writer,
        )
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
        if block:
            signal.signal(signal.SIGINT, self.__handle_sigterm)
            signal.signal(signal.SIGTERM, self.__handle_sigterm)

        log.info(f"Start Record: {self.url}")
        if self.use_credentials:
            log.info("Using Credentials")

        self.record_thread = threading.Thread(target=self.__record)
        self.record_thread.name = f"Thread-StreamRecorder-{self.platform_type.value}-{self.uid}"
        self.record_thread.start()

        if block:
            while True:
                if self.env.env == "dev":
                    input("Press any key to exit")
                    self.cancel()
                    self.__check_closed()
                    break
                if self.is_done:
                    self.__check_closed()
                    break
                time.sleep(1)
            log.info("Done")

    def __handle_sigterm(self, *acrgs):
        self.streamlink.check_tmp_dir()

    def __check_closed(self):
        if self.record_thread:
            self.record_thread.join()
        if self.amqp_thread:
            self.amqp_thread.join()

    def __record(self):
        try:
            self.__record_once()
            time.sleep(RECORDING_SHUTDOWN_SEC)
            self.__close()
            log.info("End Recording", {"latest_state": self.streamlink.state.name})
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

        if self.vid_name is not None:
            if self.cancel_flag:
                self.__publish_done(DoneStatus.CANCELED, self.vid_name)
            else:
                self.__publish_done(DoneStatus.COMPLETE, self.vid_name)

    def __record_once(self):
        streams = self.streamlink.wait_for_live()
        if streams is None:
            log.error("Stream is None")
            return

        if self.__is_recording_active():
            log.info("Recording is already in progress, skipping recording")
            return

        # Start AMQP consumer thread
        self.amqp_thread = threading.Thread(target=self.listener.consume)
        self.amqp_thread.name = f"Thread-RecorderListener-{self.platform_type.value}-{self.uid}"
        self.amqp_thread.daemon = True  # AMQP connection should be released on abnormal termination
        self.amqp_thread.start()

        # Start recording
        vid_name = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.vid_name = vid_name
        self.streamlink.record(streams, vid_name)

    def __is_recording_active(self):
        vid_queue_name = f"{EXIT_QUEUE_PREFIX}.{self.platform_type.value}.{self.uid}"
        conn, chan = self.amqp.connect()
        exists = self.amqp.queue_exists(chan, vid_queue_name)
        self.amqp.close(conn)
        return exists

    def __publish_done(self, status: DoneStatus, vid_name: str):
        msg = DoneMessage(
            status=status,
            platform=self.platform_type,
            uid=self.uid,
            video_name=vid_name,
            fs_name=self.env.fs_name,
        ).model_dump_json(by_alias=True)
        conn, chan = self.amqp.connect()
        self.amqp.ensure_queue(chan, DONE_QUEUE_NAME, auto_delete=False)
        self.amqp.publish(chan, DONE_QUEUE_NAME, msg.encode("utf-8"))
        self.amqp.close(conn)
        log.info("Published Done Message")
