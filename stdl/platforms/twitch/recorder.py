from typing import Optional

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs


class TwitchLiveRecorder(StreamRecorder):

    def __init__(
            self,
            channel_name: str,
            out_dir_path: str,
            tmp_dir_path: str,
            once: bool,
            cookies: Optional[str],
            amqp: Amqp,
    ):
        args = StreamlinkArgs(
            url=f"https://www.twitch.tv/{channel_name}",
            name=channel_name,
            out_dir_path=out_dir_path,
            tmp_dir_path=tmp_dir_path,
            cookies=cookies,
        )
        super().__init__(args, once, amqp)
