from stdl.common.amqp import AmqpHelper
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder, RecorderArgs
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.utils.fs.fs_common_abstract import FsAccessor


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
        self,
        uid: str,
        out_dir_path: str,
        cookies: str | None,
        fs_accessor: FsAccessor,
        ephemeral_amqp: AmqpHelper,
        consumer_amqp: AmqpHelper,
    ):
        url = f"https://chzzk.naver.com/live/{uid}"
        super().__init__(
            stream_args=StreamlinkArgs(url=url, uid=uid, cookies=cookies),
            recorder_args=RecorderArgs(out_dir_path=out_dir_path, platform_type=PlatformType.CHZZK),
            fs_accessor=fs_accessor,
            ephemeral_amqp=ephemeral_amqp,
            consumer_amqp=consumer_amqp,
        )
