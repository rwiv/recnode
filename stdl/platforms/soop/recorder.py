from streamlink.plugins.soop import Soop

from stdl.common.amqp import AmqpHelper
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder, RecorderArgs
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.platforms.soop.types import SoopCredential
from stdl.utils.fs.fs_common_abstract import FsAccessor


class SoopLiveRecorder(StreamRecorder):

    def __init__(
        self,
        user_id: str,
        out_dir_path: str,
        cred: SoopCredential | None,
        fs_accessor: FsAccessor,
        ephemeral_amqp: AmqpHelper,
        consumer_amqp: AmqpHelper,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        if cred is not None:
            sargs = StreamlinkArgs(url=url, uid=user_id, options=cred.to_dict())
        else:
            sargs = StreamlinkArgs(url=url, uid=user_id)
        super().__init__(
            stream_args=sargs,
            recorder_args=RecorderArgs(out_dir_path=out_dir_path, platform_type=PlatformType.SOOP),
            fs_accessor=fs_accessor,
            ephemeral_amqp=ephemeral_amqp,
            consumer_amqp=consumer_amqp,
        )

    def clear_cookie(self):
        session = self.streamlink.get_session()
        Soop(session, self.streamlink.url).clear_cookies()
