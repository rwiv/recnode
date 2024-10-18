from typing import Optional

from streamlink.plugins.afreeca import AfreecaTV

from stdl.downloaders.streamlink.recorder import StreamRecorder
from stdl.downloaders.streamlink.stream import StreamlinkArgs
from stdl.platforms.afreeca.types import AfreecaCredential


class AfreecaLiveRecorder(StreamRecorder):

    def __init__(
            self,
            user_id: str,
            out_dir_path: str,
            tmp_dir_path: str,
            once: bool,
            cred: Optional[AfreecaCredential] = None,
    ):
        args = StreamlinkArgs(
            url=f"https://play.afreecatv.com/{user_id}",
            name=user_id,
            out_dir_path=out_dir_path,
            options=cred.to_options(),
        )
        super().__init__(args, tmp_dir_path, once)

    def clear_cookie(self):
        session = self.streamlink.get_session()
        AfreecaTV(session, self.streamlink.url).clear_cookies()
