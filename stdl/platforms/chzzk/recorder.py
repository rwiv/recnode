from typing import Optional

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder, RecorderArgs
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
        sargs = StreamlinkArgs(url=url, uid=uid, cookies=cookies)
        rargs = RecorderArgs(out_dir_path=out_dir_path, platform_type=PlatformType.CHZZK, once=once)
        super().__init__(sargs, rargs, amqp)
