from .live_recorder import LiveRecorder
from ..schema.recording_arguments import StreamArgs, StreamLinkSessionArgs, RecordingArgs
from ..schema.recording_schema import StreamInfo
from ...common.env import Env
from ...common.spec import PlatformType
from ...data.live import LiveState
from ...file import ObjectWriter


class RecorderResolver:
    def __init__(self, env: Env, writer: ObjectWriter):
        self.env = env
        self.writer = writer

    def create_recorder(self, state: LiveState) -> LiveRecorder:
        if state.platform == PlatformType.CHZZK:
            return self.__create_chzzk_recorder(state)
        elif state.platform == PlatformType.SOOP:
            return self.__create_soop_recorder(state)
        elif state.platform == PlatformType.TWITCH:
            return self.__create_twitch_recorder(state)
        else:
            raise ValueError("Invalid Request Type")

    def __create_chzzk_recorder(self, state: LiveState):
        return self.__create_recorder(
            uid=state.channel_id,
            url=f"https://chzzk.naver.com/live/{state.channel_id}",
            platform=state.platform,
            cookie_header=state.cookie,
        )

    def __create_soop_recorder(self, state: LiveState):
        return self.__create_recorder(
            uid=state.channel_id,
            url=f"https://play.sooplive.co.kr/{state.channel_id}",
            platform=state.platform,
            cookie_header=state.cookie,
        )

    def __create_twitch_recorder(self, state: LiveState):
        return self.__create_recorder(
            uid=state.channel_id,
            url=f"https://www.twitch.tv/{state.channel_id}",
            platform=state.platform,
            cookie_header=state.cookie,
        )

    def __create_recorder(
        self, uid: str, url: str, platform: PlatformType, cookie_header: str | None
    ) -> LiveRecorder:
        return LiveRecorder(
            env=self.env,
            stream_args=StreamArgs(
                info=StreamInfo(uid=uid, url=url, platform=platform),
                session_args=StreamLinkSessionArgs(
                    cookie_header=cookie_header,
                    stream_timeout_sec=self.env.stream.stream_timeout_sec,
                ),
                tmp_dir_path=self.env.tmp_dir_path,
                seg_size_mb=self.env.stream.seg_size_mb,
            ),
            recording_args=RecordingArgs(
                out_dir_path=self.env.out_dir_path,
                use_credentials=cookie_header is not None,
            ),
            writer=self.writer,
        )
