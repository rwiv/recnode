import asyncio
import json
import os
import time

from dacite import WrongTypeError

from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.downloaders.ytdl.downloader import YtdlDownloader
from stdl.platforms.afreeca.recorder import AfreecaLiveRecorder
from stdl.platforms.chzzk.recorder import ChzzkLiveRecorder
from stdl.platforms.chzzk.video_downloader import ChzzkVideoDownloader
from stdl.platforms.chzzk.video_downloader_legacy import ChzzkVideoDownloaderLegacy
from stdl.platforms.twitch.recorder import TwitchLiveRecorder
from stdl.config.config import read_app_config
from stdl.config.env import get_env
from stdl.config.requests import RequestType
from stdl.utils.http import get_headers
from stdl.utils.logger import log
from stdl.utils.streamlink import disable_streamlink_log
from stdl.utils.type import convert_time
from stdl.utils.url import get_query_string


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
        elif self.conf.req_type() == RequestType.AFREECA_LIVE:
            self.run_afreeca_live()
        elif self.conf.req_type() == RequestType.TWITCH_LIVE:
            self.run_twitch_live()
        elif self.conf.req_type() == RequestType.YTDL_VIDEO:
            self.run_ytdl_video()
        elif self.conf.req_type() == RequestType.HLS_M3U8:
            self.run_hls_m3u8()
        else:
            raise ValueError("Invalid Request Type", self.conf.reqType)

    def run_hls_m3u8(self):
        req = self.conf.hlsM3u8
        if req.cookies is not None:
            headers = get_headers(json.loads(req.cookies))
        else:
            headers = get_headers()
        hls = HlsDownloader(base_dir_path=self.conf.outDirPath, headers=headers)
        for i, m3u8_url in enumerate(req.urls):
            qs = get_query_string(m3u8_url)
            title = f"hls_{i}"
            asyncio.run(hls.download(m3u8_url, title, qs))
        print("end")

    def run_ytdl_video(self):
        yt = YtdlDownloader(self.conf.outDirPath)
        yt.download(self.conf.youtubeVideo.urls)
        print("end")

    def run_chzzk_video(self):
        dl = ChzzkVideoDownloader(self.conf.outDirPath, self.conf.chzzkVideo.cookies)
        dl_l = ChzzkVideoDownloaderLegacy(self.conf.outDirPath, self.conf.chzzkVideo.cookies)
        try:
            dl.download(self.conf.chzzkVideo.videoNoList)
        except WrongTypeError:
            dl_l.download(self.conf.chzzkVideo.videoNoList)
        print("end")

    def run_chzzk_live(self):
        disable_streamlink_log()
        log.info("Conf", self.conf.to_dict())
        req = self.conf.chzzkLive
        recorder = ChzzkLiveRecorder(req.uid, self.conf.outDirPath, req.cookies)
        recorder.observe()
        self.wait(recorder.state.name)

    def run_afreeca_live(self):
        disable_streamlink_log()
        log.info("Conf", self.conf.to_dict())
        req = self.conf.afreecaLive
        recorder = AfreecaLiveRecorder(
            req.userId, self.conf.outDirPath, self.env.afreeca_credential
        )
        recorder.observe()
        self.wait(recorder.state.name)

    def run_twitch_live(self):
        disable_streamlink_log()
        log.info("Conf", self.conf.to_dict())
        req = self.conf.twitchLive
        recorder = TwitchLiveRecorder(req.channelName, self.conf.outDirPath, req.cookies)
        recorder.observe()
        self.wait(recorder.state.name)

    def wait(self, state: str):
        idx = 0
        while True:
            if idx % 10 == 0:
                log.info("Running App...", {
                    "time": convert_time(idx),
                    "state": state,
                })
            time.sleep(1)
            idx += 1
