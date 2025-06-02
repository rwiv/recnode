from pyutils import path_join
from redis.asyncio import Redis

from ..schema.recording_arguments import RecordingArgs
from ..stream.stream_recorder import StreamRecorder
from ..stream.stream_recorder_seg import SegmentedStreamRecorder
from ...common import PlatformType
from ...config import Env
from ...data.live import LiveState
from ...data.redis import create_redis_pool
from ...file import ObjectWriter
from ...utils import StreamLinkSessionArgs


class RecorderResolver:
    def __init__(self, env: Env, writer: ObjectWriter):
        self.env = env
        self.writer = writer

    def create_recorder(self, state: LiveState) -> StreamRecorder:
        if state.platform == PlatformType.CHZZK:
            return self.__create_chzzk_recorder(state)
        elif state.platform == PlatformType.SOOP:
            return self.__create_soop_recorder(state)
        elif state.platform == PlatformType.TWITCH:
            return self.__create_twitch_recorder(state)
        else:
            raise ValueError("Invalid Request Type")

    def __create_chzzk_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://chzzk.naver.com/live/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_soop_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://play.sooplive.co.kr/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_twitch_recorder(self, state: LiveState):
        cookie_header = None
        if state.headers is not None:
            cookie_header = state.headers.get("Cookie")
        return self.__create_recorder(
            state=state,
            url=f"https://www.twitch.tv/{state.channel_id}",
            cookie_header=cookie_header,
        )

    def __create_recorder(self, state: LiveState, url: str, cookie_header: str | None) -> StreamRecorder:
        return SegmentedStreamRecorder(
            live=state,
            args=RecordingArgs(
                live_url=url,
                session_args=StreamLinkSessionArgs(
                    cookie_header=cookie_header,
                    stream_timeout_sec=self.env.stream.stream_timeout_sec,
                ),
                tmp_dir_path=self.env.tmp_dir_path,
                seg_size_mb=self.env.stream.seg_size_mb,
            ),
            incomplete_dir_path=path_join(self.env.out_dir_path, "incomplete"),
            writer=self.writer,
            redis=Redis(connection_pool=create_redis_pool(self.env.redis_master)),
            redis_data_conf=self.env.redis_data,
            req_conf=self.env.req_conf,
        )
