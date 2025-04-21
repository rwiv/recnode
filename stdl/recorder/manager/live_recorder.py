import asyncio
import os
import signal
import threading
import time
from datetime import datetime
from pathlib import Path

from pyutils import log, path_join, error_dict

from ..schema.recording_arguments import StreamArgs, RecordingArgs
from ..stream.stream_recorder_seg import SegmentedStreamRecorder
from ...common.env import Env
from ...common.fs import ObjectWriter


class LiveRecorder:
    def __init__(
        self,
        env: Env,
        stream_args: StreamArgs,
        recording_args: RecordingArgs,
        writer: ObjectWriter,
    ):
        self.env = env
        self.channel_id = stream_args.info.uid
        self.url = stream_args.info.url
        self.platform = stream_args.info.platform
        self.use_credentials = recording_args.use_credentials

        self.tmp_dir_path = stream_args.tmp_dir_path
        self.incomplete_dir_path = writer.normalize_base_path(
            path_join(recording_args.out_dir_path, "incomplete")
        )

        self.dir_clear_timeout_sec = 180
        self.dir_clear_wait_delay_sec = 1

        # self.stream = StreamlinkStreamRecorder(
        self.stream = SegmentedStreamRecorder(
            args=stream_args,
            incomplete_dir_path=self.incomplete_dir_path,
            writer=writer,
        )

        self.vid_name: str | None = None
        self.is_done = False
        self.recording_thread: threading.Thread | None = None

    def get_state(self):
        return self.stream.get_status()

    def record(self, block: bool = True):
        if block:
            signal.signal(signal.SIGINT, self.__handle_signal)
            signal.signal(signal.SIGTERM, self.__handle_signal)

        self.recording_thread = threading.Thread(target=self.__record_stream)
        self.recording_thread.name = f"Thread-StreamRecorder-{self.platform.value}-{self.channel_id}"
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

    def __record_stream(self):
        try:
            self.vid_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            asyncio.run(self.stream.record(video_name=self.vid_name))
        except Exception as e:
            log.error("Recording failed", error_dict(e))
        finally:
            self.__close()

    def __close(self):
        self.is_done = True

    def __wait_for_clear_dir(self):
        if self.vid_name is None:
            return

        start_time = time.time()
        while True:
            if time.time() - start_time > self.dir_clear_timeout_sec:
                log.error("Timeout waiting for tmp dir to be cleared")
                return

            if Path(path_join(self.tmp_dir_path, self.channel_id, self.vid_name)).exists():
                log.debug("Waiting for tmp dir to be cleared")
                time.sleep(self.dir_clear_wait_delay_sec)
                continue

            chunks_dir_path = path_join(self.incomplete_dir_path, self.channel_id, self.vid_name)
            if not Path(chunks_dir_path).exists():
                break
            if len(os.listdir(chunks_dir_path)) == 0:
                os.rmdir(chunks_dir_path)
                break

            log.debug("Waiting for chunks dir to be cleared")
            time.sleep(self.dir_clear_wait_delay_sec)

        channel_dir_path = path_join(self.incomplete_dir_path, self.channel_id)
        if Path(channel_dir_path).exists() and len(os.listdir(channel_dir_path)) == 0:
            os.rmdir(channel_dir_path)

    def __handle_signal(self, *acrgs):
        self.stream.check_tmp_dir()
