from .live_recorder import LiveRecorder
from ..schema.recording_arguments import StreamArgs, StreamLinkSessionArgs, RecordingArgs
from ..schema.recording_schema import StreamInfo
from ...common.env import Env
from ...common.request import RequestType, AppRequest
from ...common.spec import PlatformType
from ...file import ObjectWriter


class RecorderResolver:
    def __init__(self, env: Env, req: AppRequest, writer: ObjectWriter):
        self.env = env
        self.req = req
        self.writer = writer

    def create_recorder(self) -> LiveRecorder:
        if self.req.req_type == RequestType.CHZZK_LIVE:
            return self.__create_chzzk_recorder()
        elif self.req.req_type == RequestType.SOOP_LIVE:
            return self.__create_soop_recorder()
        elif self.req.req_type == RequestType.TWITCH_LIVE:
            return self.__create_twitch_recorder()
        else:
            raise ValueError("Invalid Request Type")

    def __create_chzzk_recorder(self):
        req = self.req.chzzk_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return self.__create_recorder(
            uid=req.uid,
            url=f"https://chzzk.naver.com/live/{req.uid}",
            platform=PlatformType.CHZZK,
            cookies=req.cookies,
        )

    def __create_soop_recorder(self):
        req = self.req.soop_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return self.__create_recorder(
            uid=req.user_id,
            url=f"https://play.sooplive.co.kr/{req.user_id}",
            platform=PlatformType.SOOP,
            cookies=req.cookies,
        )

    def __create_twitch_recorder(self):
        req = self.req.twitch_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return self.__create_recorder(
            uid=req.channel_name,
            url=f"https://www.twitch.tv/{req.channel_name}",
            platform=PlatformType.TWITCH,
            cookies=req.cookies,
        )

    def __create_recorder(
        self, uid: str, url: str, platform: PlatformType, cookies: str | None
    ) -> LiveRecorder:
        return LiveRecorder(
            env=self.env,
            stream_args=StreamArgs(
                info=StreamInfo(uid=uid, url=url, platform=platform),
                session_args=StreamLinkSessionArgs(
                    stream_timeout_sec=self.env.stream.stream_timeout_sec,
                    cookies=cookies,
                ),
                tmp_dir_path=self.env.tmp_dir_path,
                seg_size_mb=self.env.stream.seg_size_mb,
            ),
            recording_args=RecordingArgs(
                out_dir_path=self.env.out_dir_path,
                use_credentials=cookies is not None,
            ),
            writer=self.writer,
        )
