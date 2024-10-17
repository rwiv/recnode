from typing import Optional

from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
            self,
            uid: str,
            out_dir: str,
            cookies: Optional[str] = None,
    ):
        args = StreamlinkArgs(
            url=f"https://chzzk.naver.com/live/{uid}",
            name=uid,
            out_dir=out_dir,
            cookies=cookies,
        )
        super().__init__(args)
