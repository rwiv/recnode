from ..recorder.recorder import LiveRecorder
from ..spec.recording_arguments import StreamArgs, RecordingArgs, StreamLinkSessionArgs
from ..spec.recording_schema import StreamInfo
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.spec import PlatformType


class ChzzkLiveRecorder(LiveRecorder):
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
            stream_args=StreamArgs(
                info=StreamInfo(uid=uid, url=url, platform=PlatformType.CHZZK),
                session_args=StreamLinkSessionArgs(
                    stream_timeout_sec=env.stream.stream_timeout_sec,
                    cookies=cookies,
                ),
                tmp_dir_path=env.tmp_dir_path,
                seg_size_mb=env.stream.seg_size_mb,
            ),
            recording_args=RecordingArgs(
                out_dir_path=env.out_dir_path,
                use_credentials=cookies is not None,
            ),
            writer=writer,
            amqp_helper=create_amqp(env),
        )
