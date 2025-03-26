from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecordingArgs
from ..spec.recording_schema import StreamInfo
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType


class SoopLiveRecorder(StreamRecorder):

    def __init__(
        self,
        env: Env,
        user_id: str,
        writer: ObjectWriter,
        cookies: str | None = None,
    ):
        url = f"https://play.sooplive.co.kr/{user_id}"
        super().__init__(
            env=env,
            stream_args=StreamlinkArgs(
                info=StreamInfo(uid=user_id, url=url, platform=PlatformType.SOOP),
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.seg_size_mb,
                cookies=cookies,
            ),
            recorder_args=RecordingArgs(
                out_dir_path=env.out_dir_path,
                use_credentials=cookies is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )
