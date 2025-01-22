from typing import Optional

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
            self,
            uid: str,
            out_dir_path: str,
            once: bool,
            cookies: Optional[str],
            amqp: Amqp,
    ):
        url = f"https://chzzk.naver.com/live/{uid}"
        args = StreamlinkArgs(url=url, name=uid, cookies=cookies)
        super().__init__(args, out_dir_path, once, amqp)
