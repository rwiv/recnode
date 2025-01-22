from typing import Optional

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs


class TwitchLiveRecorder(StreamRecorder):

    def __init__(
            self,
            channel_name: str,
            out_dir_path: str,
            once: bool,
            cookies: Optional[str],
            amqp: Amqp,
    ):
        url = f"https://www.twitch.tv/{channel_name}"
        args = StreamlinkArgs(url=url, name=channel_name, cookies=cookies)
        super().__init__(args, out_dir_path, once, amqp)
