from ..recorder.recorder_impl import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType


class TwitchLiveRecorder(StreamRecorder):

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
            stream_args=StreamlinkArgs(url=url, uid=channel_name, cookies=cookies),
            recorder_args=RecorderArgs(
                out_dir_path=env.out_dir_path,
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.seg_size_mb,
                platform_type=PlatformType.TWITCH,
                use_credentials=cookies is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )
