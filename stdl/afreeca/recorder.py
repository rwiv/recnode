from typing import Optional

from streamlink.plugins.afreeca import AfreecaTV

from stdl.common.recorder import StreamRecorder, StreamRecorderArgs
from stdl.afreeca.types import AfreecaCredential


class AfreecaLiveRecorder(StreamRecorder):

    def __init__(
            self,
            user_id: str,
            out_dir: str,
            cred: Optional[AfreecaCredential] = None,
            wait_interval: int = 1,
            restart_delay: int = 40,
    ):
        args = StreamRecorderArgs(
            url=f"https://play.afreecatv.com/{user_id}",
            name=user_id,
            out_dir=out_dir,
            options=cred.to_options(),
            wait_interval=wait_interval,
            restart_delay=restart_delay,
        )
        super().__init__(args)

    def clear_cookie(self):
        AfreecaTV(self.get_session(), self.url).clear_cookies()
