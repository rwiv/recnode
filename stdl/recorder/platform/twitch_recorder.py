from ..manager.live_recorder import LiveRecorder
from ..schema.recording_arguments import StreamArgs, RecordingArgs, StreamLinkSessionArgs
from ..schema.recording_schema import StreamInfo
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType


class TwitchLiveRecorder(LiveRecorder):
    def __init__(
        self,
        env: Env,
        channel_name: str,
        writer: ObjectWriter,
        cookies: str | None = None,
    ):
        url = f"https://www.twitch.tv/{channel_name}"
        super().__init__(
            env=env,
            stream_args=StreamArgs(
                info=StreamInfo(uid=channel_name, url=url, platform=PlatformType.TWITCH),
                session_args=StreamLinkSessionArgs(
                    stream_timeout_sec=env.stream.stream_timeout_sec,
                    cookies=cookies,
                ),
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.stream.seg_size_mb,
            ),
            recording_args=RecordingArgs(
                out_dir_path=env.out_dir_path,
                use_credentials=cookies is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )
