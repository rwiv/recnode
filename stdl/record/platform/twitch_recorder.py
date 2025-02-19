from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ...common import AmqpHelper, PlatformType
from stdl.utils.fs.fs_common_abstract import FsAccessor


class TwitchLiveRecorder(StreamRecorder):

    def __init__(
        self,
        channel_name: str,
        out_dir_path: str,
        cookies: str | None,
        fs_accessor: FsAccessor,
        amqp_helper: AmqpHelper,
    ):
        url = f"https://www.twitch.tv/{channel_name}"
        super().__init__(
            stream_args=StreamlinkArgs(url=url, uid=channel_name, cookies=cookies),
            recorder_args=RecorderArgs(
                out_dir_path=out_dir_path,
                platform_type=PlatformType.TWITCH,
                use_credentials=cookies is not None,
            ),
            fs_accessor=fs_accessor,
            amqp_helper=amqp_helper,
        )
