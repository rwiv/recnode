from typing import Optional

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.recorder import StreamRecorder, RecorderArgs
from stdl.downloaders.streamlink.stream import StreamlinkArgs


class TwitchLiveRecorder(StreamRecorder):

    def __init__(
            self,
            channel_name: str,
            out_dir_path: str,
            cookies: Optional[str],
            pub: Amqp,
            sub: Amqp,
    ):
        url = f"https://www.twitch.tv/{channel_name}"
        sargs = StreamlinkArgs(url=url, uid=channel_name, cookies=cookies)
        rargs = RecorderArgs(out_dir_path=out_dir_path, platform_type=PlatformType.TWITCH)
        super().__init__(sargs, rargs, pub, sub)
