import asyncio
import signal
import threading
import time

from pyutils import log, path_join, error_dict

from ..schema.recording_arguments import StreamArgs, RecordingArgs
from ..stream.stream_recorder_seg import SegmentedStreamRecorder
from ...common.env import Env
from ...data.live import LiveState
from ...file import ObjectWriter


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
        self.incomplete_dir_path = path_join(recording_args.out_dir_path, "incomplete")

        self.dir_clear_timeout_sec = 180
        self.dir_clear_wait_delay_sec = 1

        # self.stream = StreamlinkStreamRecorder(
        self.stream = SegmentedStreamRecorder(
            args=stream_args,
            incomplete_dir_path=self.incomplete_dir_path,
            writer=writer,
            req_conf=self.env.req_conf,
        )

        self.vid_name: str | None = None
        self.is_done = False
        self.recording_thread: threading.Thread | None = None

    def get_status(self, with_stats: bool = False, full_stats: bool = False):
        return self.stream.get_status(with_stats=with_stats, full_stats=full_stats)

    def record(self, state: LiveState, block: bool = True):
        self.recording_thread = threading.Thread(target=self.__record_stream, args=(state,))
        self.recording_thread.name = f"Thread-StreamRecorder-{self.platform.value}-{self.channel_id}"
        self.recording_thread.start()

        if self.env.env == "dev" and block:
            signal.signal(signal.SIGINT, self.__handle_signal)
            signal.signal(signal.SIGTERM, self.__handle_signal)

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

    def __record_stream(self, state: LiveState):
        try:
            asyncio.run(self.stream.record(state))
        except Exception as e:
            log.error("Recording failed", error_dict(e))
        finally:
            self.__close()

    def __close(self):
        self.is_done = True

    def __handle_signal(self, *acrgs):
        self.stream.check_tmp_dir()
