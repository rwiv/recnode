from streamlink.plugins.soop import Soop

from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecorderArgs
from ...common.amqp import AmqpHelper, create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.request.request_types import SoopCredential
from ...common.spec import PlatformType


class SoopLiveRecorder(StreamRecorder):

    def __init__(
        self,
        env: Env,
        user_id: str,
        writer: ObjectWriter,
        cred: SoopCredential | None = None,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        if cred is not None:
            sargs = StreamlinkArgs(url=url, uid=user_id, options=cred.to_dict())
        else:
            sargs = StreamlinkArgs(url=url, uid=user_id)
        super().__init__(
            env=env,
            stream_args=sargs,
            recorder_args=RecorderArgs(
                out_dir_path=env.out_dir_path,
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.seg_size_mb,
                platform_type=PlatformType.SOOP,
                use_credentials=cred is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )

    def clear_cookie(self):
        session = self.streamlink.get_session()
        Soop(session, self.streamlink.url).clear_cookies()
