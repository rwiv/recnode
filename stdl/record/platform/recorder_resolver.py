from pynifs import FsAccessor

from .chzzk_recorder import ChzzkLiveRecorder
from .soop_recorder import SoopLiveRecorder
from .twitch_recorder import TwitchLiveRecorder
from ..recorder.recorder import StreamRecorder
from ...common.amqp import create_amqp
from ...common.env import Env
from ...common.request import RequestType, AppConfig


class RecorderResolver:
    def __init__(self, env: Env, req: AppConfig, ac: FsAccessor):
        self.env = env
        self.req = req
        self.ac = ac

    def create_recorder(self) -> StreamRecorder:
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
        return ChzzkLiveRecorder(
            req.uid,
            self.env.out_dir_path,
            req.cookies,
            self.ac,
            create_amqp(self.env),
        )

    def __create_soop_recorder(self):
        req = self.req.soop_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return SoopLiveRecorder(
            req.user_id,
            self.env.out_dir_path,
            req.cred,
            self.ac,
            create_amqp(self.env),
        )

    def __create_twitch_recorder(self):
        req = self.req.twitch_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return TwitchLiveRecorder(
            req.channel_name,
            self.env.out_dir_path,
            req.cookies,
            self.ac,
            create_amqp(self.env),
        )
