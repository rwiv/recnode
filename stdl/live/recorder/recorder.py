import os
import signal
import threading
import time
from datetime import datetime
from pathlib import Path

from pyutils import log, path_join, error_dict

from .stream_listener import EXIT_QUEUE_PREFIX
from .stream_manager import StreamManager
from ..spec.done_message import DoneStatus, DoneMessage
from ..spec.recording_arguments import StreamArgs, RecordingArgs
from ..spec.recording_constants import DONE_QUEUE_NAME
from ..spec.recording_schema import RecorderStatusInfo
from ...common.amqp import AmqpHelper
from ...common.env import Env
from ...common.fs import ObjectWriter


class LiveRecorder:
    def __init__(
        self,
        env: Env,
        stream_args: StreamArgs,
        recording_args: RecordingArgs,
        writer: ObjectWriter,
        amqp_helper: AmqpHelper,
    ):
        self.env = env
        self.uid = stream_args.info.uid
        self.url = stream_args.info.url
        self.platform = stream_args.info.platform
        self.use_credentials = recording_args.use_credentials

        self.tmp_dir_path = stream_args.tmp_dir_path
        self.incomplete_dir_path = writer.normalize_base_path(
            path_join(recording_args.out_dir_path, "incomplete")
        )

        self.dir_clear_timeout_sec = 180
        self.dir_clear_wait_delay_sec = 1

        self.stream = StreamManager(
            args=stream_args,
            incomplete_dir_path=self.incomplete_dir_path,
            writer=writer,
            amqp_helper=amqp_helper,
        )
        self.amqp = amqp_helper

        self.vid_name: str | None = None
        self.is_done = False
        self.recording_thread: threading.Thread | None = None

    def get_state(self):
        return RecorderStatusInfo(
            platform=self.platform,
            uid=self.uid,
            idx=self.stream.idx,
            streamStatus=self.stream.status,
        )

    def record(self, block: bool = True):
        if block:
            signal.signal(signal.SIGINT, self.__handle_signal)
            signal.signal(signal.SIGTERM, self.__handle_signal)

        log.info(f"Start Record: {self.url}")
        if self.use_credentials:
            log.info("Using Credentials")

        self.recording_thread = threading.Thread(target=self.__record_stream)
        self.recording_thread.name = f"Thread-StreamRecorder-{self.platform.value}-{self.uid}"
        self.recording_thread.start()

        if block:
            while True:
                if self.env.env == "dev":
                    input("Press any key to exit")
                    self.stream.state.cancel()
                    self.recording_thread.join()
                    break
                if self.is_done:
                    self.recording_thread.join()
                    break
                time.sleep(1)
            log.info("Done")

    def __record_stream(self):
        try:
            # Wait until the live streams is obtained
            streams = self.stream.wait_for_live()
            if streams is None:
                log.error("Failed to get live streams")
                return

            # Check if recording is already in progress
            if self.__is_already_recording():
                log.info("Recording is already in progress, skipping recording")
                return

            # Start recording
            self.vid_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.stream.record(streams, self.vid_name)

            # Wait for recording to finish
            self.__close()
            log.info("End Recording", {"latest_state": self.stream.status.name})
        except Exception as e:
            log.error("Recording failed", error_dict(e))
            self.__close()

    def __close(self):
        # Wait for tmp dir to be cleared
        if self.env.watcher.enabled:
            log.info("Waiting for dir to be cleared")
            self.__wait_for_clear_dir()
            log.info("Dir cleared")

        # Publish Done Message
        if self.vid_name is not None:
            if self.stream.state.cancel_flag:
                self.__publish_done_message(DoneStatus.CANCELED, self.vid_name)
            else:
                self.__publish_done_message(DoneStatus.COMPLETE, self.vid_name)

        # Set done flag
        self.is_done = True

    def __is_already_recording(self):
        vid_queue_name = f"{EXIT_QUEUE_PREFIX}.{self.platform.value}.{self.uid}"
        conn, chan = self.amqp.connect()
        exists = self.amqp.queue_exists(chan, vid_queue_name)
        self.amqp.close(conn)
        return exists

    def __publish_done_message(self, status: DoneStatus, vid_name: str):
        msg = DoneMessage(
            status=status,
            platform=self.platform,
            uid=self.uid,
            video_name=vid_name,
            fs_name=self.env.fs_name,
        ).model_dump_json(by_alias=True)
        conn, chan = self.amqp.connect()
        self.amqp.ensure_queue(chan, DONE_QUEUE_NAME, auto_delete=False)
        self.amqp.publish(chan, DONE_QUEUE_NAME, msg.encode("utf-8"))
        self.amqp.close(conn)
        log.info("Published Done Message")

    def __wait_for_clear_dir(self):
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

            try:
                chunks_dir_path = path_join(self.incomplete_dir_path, self.uid, self.vid_name)
                if not Path(chunks_dir_path).exists():
                    break
                if len(os.listdir(chunks_dir_path)) == 0:
                    os.rmdir(chunks_dir_path)
                    break
            except Exception as e:
                log.error("Failed to remove chunks dir", error_dict(e))
                continue

            log.debug("Waiting for chunks dir to be cleared")
            time.sleep(self.dir_clear_wait_delay_sec)

        channel_dir_path = path_join(self.incomplete_dir_path, self.uid)
        if len(os.listdir(channel_dir_path)) == 0:
            os.rmdir(channel_dir_path)

    def __handle_signal(self, *acrgs):
        self.stream.check_tmp_dir()
