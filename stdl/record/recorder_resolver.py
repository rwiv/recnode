from stdl.common.amqp_utils import create_amqp
from stdl.common.env import Env
from stdl.common.request_config import AppConfig
from stdl.common.request_types import RequestType
from stdl.record.recorder.recorder import StreamRecorder
from stdl.record.platform.chzzk_recorder import ChzzkLiveRecorder
from stdl.record.platform.soop_recorder import SoopLiveRecorder
from stdl.record.platform.twitch_recorder import TwitchLiveRecorder
from stdl.utils.fs.fs_common_abstract import FsAccessor


class RecorderResolver:
    def __init__(self, env: Env, conf: AppConfig, ac: FsAccessor):
        self.env = env
        self.conf = conf
        self.ac = ac

    def create_recorder(self) -> StreamRecorder:
        if self.conf.req_type == RequestType.CHZZK_LIVE:
            return self.__create_chzzk_recorder()
        elif self.conf.req_type == RequestType.SOOP_LIVE:
            return self.__create_soop_recorder()
        elif self.conf.req_type == RequestType.TWITCH_LIVE:
            return self.__create_twitch_recorder()
        else:
            raise ValueError("Invalid Request Type")

    def __create_chzzk_recorder(self):
        req = self.conf.chzzk_live
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
        req = self.conf.soop_live
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
        req = self.conf.twitch_live
        if req is None:
            raise ValueError("Invalid Request Type")
        return TwitchLiveRecorder(
            req.channel_name,
            self.env.out_dir_path,
            req.cookies,
            self.ac,
            create_amqp(self.env),
        )
