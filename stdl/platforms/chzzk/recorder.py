from typing import Optional

from stdl.downloaders.streamlink.recorder import StreamRecorder, StreamRecorderArgs


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
            self,
            uid: str,
            out_dir: str,
            cookies: Optional[str] = None,
            wait_interval: int = 1,
            restart_delay: int = 40,
    ):
        args = StreamRecorderArgs(
            url=f"https://chzzk.naver.com/live/{uid}",
            name=uid,
            out_dir=out_dir,
            cookies=cookies,
            wait_interval=wait_interval,
            restart_delay=restart_delay,
        )
        super().__init__(args)
