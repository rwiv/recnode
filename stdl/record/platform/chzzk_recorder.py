from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ...common.amqp import AmqpHelper
from ...common.fs import FsWriter
from ...common.spec import PlatformType


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
        self,
        uid: str,
        out_dir_path: str,
        cookies: str | None,
        writer: FsWriter,
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
            writer=writer,
            amqp_helper=amqp_helper,
        )
