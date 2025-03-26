from ..recorder.recorder import StreamRecorder
from ..spec.recording_arguments import StreamlinkArgs, RecordingArgs
from ..spec.recording_schema import StreamInfo
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType


class ChzzkLiveRecorder(StreamRecorder):

    def __init__(
        self,
        env: Env,
        uid: str,
        writer: ObjectWriter,
        cookies: str | None = None,
    ):
        url = f"https://chzzk.naver.com/live/{uid}"
        super().__init__(
            env=env,
            stream_args=StreamlinkArgs(
                info=StreamInfo(uid=uid, url=url, platform=PlatformType.CHZZK),
                cookies=cookies,
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.seg_size_mb,
            ),
            recorder_args=RecordingArgs(
                out_dir_path=env.out_dir_path,
                use_credentials=cookies is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )
