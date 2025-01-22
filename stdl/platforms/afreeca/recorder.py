from typing import Optional

from streamlink.plugins.soop import Soop

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.platforms.afreeca.types import AfreecaCredential


class AfreecaLiveRecorder(StreamRecorder):

    def __init__(
            self,
            user_id: str,
            out_dir_path: str,
            tmp_dir_path: str,
            once: bool,
            cred: Optional[AfreecaCredential],
            amqp: Amqp,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        if cred is not None:
            args = StreamlinkArgs(
                url=url, name=user_id, out_dir_path=out_dir_path, tmp_dir_path=tmp_dir_path,
                options=cred.to_dict(),
            )
        else:
            args = StreamlinkArgs(
                url=url, name=user_id, out_dir_path=out_dir_path, tmp_dir_path=tmp_dir_path,
            )
        super().__init__(args, once, amqp)

    def clear_cookie(self):
        session = self.streamlink.get_session()
        Soop(session, self.streamlink.url).clear_cookies()

