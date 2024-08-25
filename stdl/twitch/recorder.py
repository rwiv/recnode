from typing import Optional

from stdl.common.recorder import StreamRecorder, StreamRecorderArgs


class TwitchLiveRecorder(StreamRecorder):

    def __init__(
            self,
            channel_name: str,
            out_dir: str,
            cookies: Optional[str] = None,
            wait_interval: int = 1,
            restart_delay: int = 40,
    ):
        args = StreamRecorderArgs(
            url=f"https://www.twitch.tv/{channel_name}",
            name=channel_name,
            out_dir=out_dir,
            cookies=cookies,
            wait_interval=wait_interval,
            restart_delay=restart_delay,
        )
        super().__init__(args)
