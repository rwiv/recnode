from streamlink.plugins.soop import Soop

from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ...common.amqp import AmqpHelper
from ...common.fs import FsWriter
from ...common.request.request_types import SoopCredential
from ...common.spec import PlatformType


class SoopLiveRecorder(StreamRecorder):

    def __init__(
        self,
        user_id: str,
        out_dir_path: str,
        cred: SoopCredential | None,
        writer: FsWriter,
        amqp_helper: AmqpHelper,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        if cred is not None:
            sargs = StreamlinkArgs(url=url, uid=user_id, options=cred.to_dict())
        else:
            sargs = StreamlinkArgs(url=url, uid=user_id)
        super().__init__(
            stream_args=sargs,
            recorder_args=RecorderArgs(
                out_dir_path=out_dir_path,
                platform_type=PlatformType.SOOP,
                use_credentials=cred is not None,
            ),
            writer=writer,
            amqp_helper=amqp_helper,
        )

    def clear_cookie(self):
        session = self.streamlink.get_session()
        Soop(session, self.streamlink.url).clear_cookies()
