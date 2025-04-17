from ..common.env import get_env
from ..common.fs import create_fs_writer
from ..common.request import read_request_by_env, RequestType
from ..recorder import RecorderResolver, disable_streamlink_log


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.conf = read_request_by_env(self.env)
        self.writer = create_fs_writer(self.env)
        self.recorder_resolver = RecorderResolver(self.env, self.conf, self.writer)

    def run(self):
        if self.conf.req_type in {RequestType.CHZZK_LIVE, RequestType.SOOP_LIVE, RequestType.TWITCH_LIVE}:
            return self.__record_live()
        raise ValueError("Invalid Request Type")

    def __record_live(self):
        disable_streamlink_log()
        recorder = self.recorder_resolver.create_recorder()
        recorder.record(block=True)
