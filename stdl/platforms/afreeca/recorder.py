from typing import Optional

from streamlink.plugins.afreeca import AfreecaTV

from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.platforms.afreeca.types import AfreecaCredential


class AfreecaLiveRecorder(StreamRecorder):

    def __init__(
            self,
            user_id: str,
            out_dir: str,
            cred: Optional[AfreecaCredential] = None,
    ):
        args = StreamlinkArgs(
            url=f"https://play.afreecatv.com/{user_id}",
            name=user_id,
            out_dir=out_dir,
            options=cred.to_options(),
        )
        super().__init__(args)

    def clear_cookie(self):
        session = self.streamlink.get_session()
        AfreecaTV(session, self.url).clear_cookies()
