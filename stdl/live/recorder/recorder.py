import os
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from threading import Thread

from pyutils import log, path_join, error_dict

from .stream_listener import RecorderListener, EXIT_QUEUE_PREFIX
from .stream_manager import StreamManager
from ..spec.done_message import DoneStatus, DoneMessage
from ..spec.recording_arguments import StreamlinkArgs, RecordingArgs
from ..spec.recording_constants import DONE_QUEUE_NAME
from ..spec.recording_schema import RecorderStatusInfo
from ...common.amqp import AmqpHelper
from ...common.env import Env
from ...common.fs import ObjectWriter


class StreamRecorder:
    def __init__(
        self,
        env: Env,
        stream_args: StreamlinkArgs,
        recorder_args: RecordingArgs,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        self.env = env
        self.writer = writer
        self.uid = stream_args.info.uid
        self.url = stream_args.info.url
        self.platform_type = stream_args.info.platform
        self.use_credentials = recorder_args.use_credentials

        self.vid_name: str | None = None
        self.tmp_dir_path = stream_args.tmp_dir_path
        self.dir_clear_timeout_sec = 180
        self.dir_clear_wait_delay_sec = 1
        self.incomplete_dir_path = self.writer.normalize_base_path(
            path_join(recorder_args.out_dir_path, "incomplete")
        )

        self.stream = StreamManager(
            stream_args,
            self.incomplete_dir_path,
            self.writer,
            amqp_helper,
        )
        self.state = self.stream.state
        self.listener = RecorderListener(
            stream_args.info,
            self.state,
            amqp_helper,
        )
        self.amqp = amqp_helper

        self.is_done = False
        # self.cancel_flag = False

        self.record_thread: Thread | None = None
        self.amqp_thread: Thread | None = None

    def get_state(self):
        return RecorderStatusInfo(
            platform=self.platform_type,
            uid=self.uid,
            idx=self.stream.idx,
            streamStatus=self.stream.status,
        )

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
                    self.state.cancel()
                    self.__check_closed()
                    break
                if self.is_done:
                    self.__check_closed()
                    break
                time.sleep(1)
            log.info("Done")

    def __handle_sigterm(self, *acrgs):
        self.stream.check_tmp_dir()

    def __check_closed(self):
        if self.record_thread:
            self.record_thread.join()
        if self.amqp_thread:
            self.amqp_thread.join()

    def __record(self):
        try:
            self.__record_once()
            self.__close()
            log.info("End Recording", {"latest_state": self.stream.status.name})
        except Exception as e:
            log.error("Recording failed", error_dict(e))
            self.__close()

    def __close(self):
        # Close AMQP connection
        conn = self.listener.conn
        if conn is not None:

            def close_conn():
                self.listener.amqp.close(conn)

            conn.add_callback_threadsafe(close_conn)
        if self.amqp_thread is not None:
            self.amqp_thread.join()

        # Wait for tmp dir to be cleared
        if self.env.watcher.enabled:
            log.info("Waiting for dir to be cleared")
            self.wait_for_clear_dir()
            log.info("Dir cleared")

        # Publish Done Message
        if self.vid_name is not None:
            if self.state.cancel_flag:
                self.__publish_done(DoneStatus.CANCELED, self.vid_name)
            else:
                self.__publish_done(DoneStatus.COMPLETE, self.vid_name)

        # Set done flag
        self.is_done = True

    def __record_once(self):
        streams = self.stream.wait_for_live()
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
        self.stream.record(streams, vid_name)

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

    def wait_for_clear_dir(self):
        if self.vid_name is None:
            return

        start_time = time.time()
        while True:
            if time.time() - start_time > self.dir_clear_timeout_sec:
                log.error("Timeout waiting for tmp dir to be cleared")
                return

            if Path(path_join(self.tmp_dir_path, self.uid, self.vid_name)).exists():
                log.debug("Waiting for tmp dir to be cleared")
                time.sleep(self.dir_clear_wait_delay_sec)
                continue

            chunks_dir_path = path_join(self.incomplete_dir_path, self.uid, self.vid_name)
            if not Path(chunks_dir_path).exists():
                break
            if len(os.listdir(chunks_dir_path)) == 0:
                os.rmdir(chunks_dir_path)
                break

            log.debug("Waiting for chunks dir to be cleared")
            time.sleep(self.dir_clear_wait_delay_sec)

        channel_dir_path = path_join(self.incomplete_dir_path, self.uid)
        if len(os.listdir(channel_dir_path)) == 0:
            os.rmdir(channel_dir_path)
