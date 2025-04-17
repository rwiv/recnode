from .chzzk_recorder import ChzzkLiveRecorder
from .soop_recorder import SoopLiveRecorder
from .twitch_recorder import TwitchLiveRecorder
from ..recorder.live_recorder import LiveRecorder
from ...common.env import Env
from ...common.fs import ObjectWriter
from ...common.request import RequestType, AppRequest


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
        return ChzzkLiveRecorder(self.env, req.uid, self.writer, req.cookies)

    def __create_soop_recorder(self):
        req = self.req.soop_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return SoopLiveRecorder(self.env, req.user_id, self.writer, req.cookies)

    def __create_twitch_recorder(self):
        req = self.req.twitch_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return TwitchLiveRecorder(self.env, req.channel_name, self.writer, req.cookies)
