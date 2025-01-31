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
            cookies: Optional[str],
            pub: Amqp,
            sub: Amqp,
    ):
        url = f"https://chzzk.naver.com/live/{uid}"
        sargs = StreamlinkArgs(url=url, uid=uid, cookies=cookies)
        rargs = RecorderArgs(out_dir_path=out_dir_path, platform_type=PlatformType.CHZZK)
        super().__init__(sargs, rargs, pub, sub)
