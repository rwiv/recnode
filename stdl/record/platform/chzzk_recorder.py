from stdl.common.amqp import AmqpHelper
from stdl.common.types import PlatformType
from stdl.record.recorder.recorder import StreamRecorder
from stdl.record.spec.recording_arguments import StreamlinkArgs, RecorderArgs
from stdl.utils.fs.fs_common_abstract import FsAccessor


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
        self,
        uid: str,
        out_dir_path: str,
        cookies: str | None,
        fs_accessor: FsAccessor,
        amqp_helper: AmqpHelper,
    ):
        url = f"https://chzzk.naver.com/live/{uid}"
        super().__init__(
            stream_args=StreamlinkArgs(url=url, uid=uid, cookies=cookies),
            recorder_args=RecorderArgs(
                out_dir_path=out_dir_path,
                platform_type=PlatformType.CHZZK,
                use_credentials=cookies is not None,
            ),
            fs_accessor=fs_accessor,
            amqp_helper=amqp_helper,
        )
