import threading
import time

from pydantic import BaseModel
from pyutils import log, error_dict

from ..manager.recorder_resolver import RecorderResolver
from ..schema.recording_constants import SCHEDULER_CHECK_DELAY_SEC
from ..stream.stream_recorder import StreamRecorder
from ...common import PlatformType
from ...config import Env
from ...data.live import LiveState
from ...file import create_fs_writer


class RecordingSummary(BaseModel):
    platform: PlatformType
    channel_id: str
    video_name: str


class RecordingScheduler:
    def __init__(self, env: Env, my_public_ip: str):
        self.__env = env
        self.__writer = create_fs_writer(self.__env)
        self.__resolver = RecorderResolver(self.__env, self.__writer, my_public_ip)

        self.__recorder_map: dict[str, StreamRecorder] = {}
        self.__check_thread: threading.Thread | None = None
        self.__start_monitoring_states()

    async def get_status(self, with_stats: bool = False, full_stats: bool = False, with_resources: bool = False):
        recordings = [
            await recorder.get_status(with_stats=with_stats, full_stats=full_stats)
            for recorder in self.__recorder_map.values()
        ]
        result: dict = {"recordings": recordings}
        if with_resources:
            result["thread_counts"] = len(threading.enumerate())
            result["thread_names"] = [thread.name for thread in threading.enumerate()]
        return result

    def get_recorder_summaries(self):
        result: list[RecordingSummary] = []
        for recorder in self.__recorder_map.values():
            summary = RecordingSummary(
                platform=recorder.ctx.live.platform,
                channel_id=recorder.ctx.live.channel_id,
                video_name=recorder.ctx.video_name,
            )
            result.append(summary)
        return result

    def record(self, state: LiveState):
        recorder = self.__resolver.create_recorder(state)
        key = _parse_key(state)
        if self.__recorder_map.get(key):
            log.info("Already Recording")
            return
        self.__recorder_map[key] = recorder
        recorder.record()

    def cancel(self, state: LiveState):
        recorder = self.__recorder_map.get(_parse_key(state))
        if recorder is not None:
            recorder.cancel()
        else:
            log.error(f"Not found recorder", state.model_dump(mode="json"))

    def __start_monitoring_states(self):
        self.__check_thread = threading.Thread(target=self.__monitor_states)
        self.__check_thread.daemon = True
        self.__check_thread.start()

    def __monitor_states(self):
        while True:
            try:
                keys = list(self.__recorder_map.keys())
                for key in keys:
                    recorder = self.__recorder_map.get(key)
                    if recorder is not None and recorder.is_done:
                        if recorder.recording_thread is not None:
                            recorder.recording_thread.join()
                        log.info(f"Remove Done Recorder", recorder.ctx.to_dict())
                        del self.__recorder_map[key]
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)
            except Exception as e:
                log.error("Failed to monitor states", error_dict(e))
                time.sleep(SCHEDULER_CHECK_DELAY_SEC)


def _parse_key(state: LiveState) -> str:
    return f"{state.platform.value}:{state.channel_id}:{state.video_name}"
