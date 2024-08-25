import os
import time
from stdl.config.config import read_app_config
from stdl.config.env import get_env
from stdl.config.requests import RequestType
from stdl.utils.logger import log
from stdl.chzzk_vid.recorder import StreamRecorder
from stdl.utils.streamlink import disable_streamlink_log
from stdl.utils.type import convert_time
from stdl.youtube.downloader import YoutubeDownloader


class Runner:
    def __init__(self):
        self.env = get_env()
        self.conf = read_app_config(self.env.config_path)

    def run(self):
        os.makedirs(self.conf.outDirPath, exist_ok=True)
        if self.conf.req_type() == RequestType.CHZZK_LIVE:
            self.run_chzzk_live()
        elif self.conf.req_type() == RequestType.CHZZK_VIDEO:
            self.run_chzzk_video()
        elif self.conf.req_type() == RequestType.YOUTUBE_VIDEO:
            self.run_youtube_video()
        else:
            raise ValueError("Invalid Request Type", self.conf.reqType)

    def run_youtube_video(self):
        yt = YoutubeDownloader(self.conf.outDirPath)
        yt.download(self.conf.youtubeVideo.urls)
        print("end")

    def run_chzzk_video(self):
        print("hello")

    def run_chzzk_live(self):
        disable_streamlink_log()
        log.info("Conf", self.conf)
        req = self.conf.chzzkLive
        recorder = StreamRecorder(req.uid, self.conf.outDirPath, self.conf.cookies)
        recorder.observe()
        idx = 0
        while True:
            if idx % 10 == 0:
                log.info("Running App...", {
                    "time": convert_time(idx),
                    "state": recorder.state.name,
                })
            time.sleep(1)
            idx += 1

