from streamlink.plugins.soop import Soop

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder, RecorderArgs
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.platforms.soop.types import SoopCredential


class SoopLiveRecorder(StreamRecorder):

    def __init__(
        self,
        user_id: str,
        out_dir_path: str,
        cred: SoopCredential | None,
        pub: Amqp,
        sub: Amqp,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        if cred is not None:
            sargs = StreamlinkArgs(url=url, uid=user_id, options=cred.to_dict())
        else:
            sargs = StreamlinkArgs(url=url, uid=user_id)
        rargs = RecorderArgs(out_dir_path, PlatformType.SOOP)
        super().__init__(sargs, rargs, pub, sub)

    def clear_cookie(self):
        session = self.streamlink.get_session()
        Soop(session, self.streamlink.url).clear_cookies()
