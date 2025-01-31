import asyncio
import json
import os
import time

from stdl.common.amqp import AmqpBlocking, AmqpMock
from stdl.common.config import read_app_config_by_file, read_app_config_by_env
from stdl.common.env import get_env
from stdl.common.requests import RequestType
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.downloaders.ytdl.downloader import YtdlDownloader
from stdl.platforms.soop.recorder import SoopLiveRecorder
from stdl.platforms.soop.video_downloader import SoopVideoDownloader
from stdl.platforms.chzzk.recorder import ChzzkLiveRecorder
from stdl.platforms.chzzk.video_downloader import ChzzkVideoDownloader
from stdl.platforms.chzzk.video_downloader_legacy import ChzzkVideoDownloaderLegacy
from stdl.platforms.twitch.recorder import TwitchLiveRecorder
from stdl.utils.http import get_headers
from stdl.utils.logger import log
from stdl.utils.streamlink import disable_streamlink_log
from stdl.utils.url import get_query_string


class Runner:
    def __init__(self):
        self.env = get_env()
        self.conf = self.__read_config()

    def __read_config(self):
        conf = read_app_config_by_env()
        if conf is None:
            conf = read_app_config_by_file(self.env.config_path)
        return conf

    def run(self):
        if self.conf.startDelayMs > 0:
            log.info(f"Start delay: {self.conf.startDelayMs}ms")
            time.sleep(self.conf.startDelayMs / 1000)

        os.makedirs(self.env.out_dir_path, exist_ok=True)

        if self.conf.reqType == RequestType.CHZZK_LIVE:
            self.run_chzzk_live()
        elif self.conf.reqType == RequestType.CHZZK_VIDEO:
            self.run_chzzk_video()
        elif self.conf.reqType == RequestType.SOOP_LIVE:
            self.run_soop_live()
        elif self.conf.reqType == RequestType.SOOP_VIDEO:
            self.run_soop_video()
        elif self.conf.reqType == RequestType.TWITCH_LIVE:
            self.run_twitch_live()
        elif self.conf.reqType == RequestType.YTDL_VIDEO:
            self.run_ytdl_video()
        elif self.conf.reqType == RequestType.HLS_M3U8:
            self.run_hls_m3u8()
        else:
            raise ValueError("Invalid Request Type", self.conf.reqType)

    def run_hls_m3u8(self):
        req = self.conf.hlsM3u8
        if req.cookies is not None:
            headers = get_headers(json.loads(req.cookies))
        else:
            headers = get_headers()

        parallel_num = 30
        hls = HlsDownloader(
            self.env.tmp_dir_path, self.env.out_dir_path, headers, parallel_num,
        )
        for i, m3u8_url in enumerate(req.urls):
            qs = get_query_string(m3u8_url)
            title = f"hls_{i}"
            asyncio.run(hls.download_parallel(m3u8_url, "hls", title, qs))
        print("end")

    def run_ytdl_video(self):
        yt = YtdlDownloader(self.env.out_dir_path)
        yt.download(self.conf.youtubeVideo.urls)
        print("end")

    def run_chzzk_video(self):
        env = self.env
        vconf = self.conf.chzzkVideo
        dl = ChzzkVideoDownloader(env.tmp_dir_path, env.out_dir_path, vconf)
        dl_l = ChzzkVideoDownloaderLegacy(env.tmp_dir_path, env.out_dir_path, vconf)
        for video_no in vconf.videoNoList:
            try:
                dl.download_one(video_no)
            except TypeError:
                dl_l.download_one(video_no)
        print("end")

    def run_soop_video(self):
        env = self.env
        vconf = self.conf.soopVideo
        dl = SoopVideoDownloader(env.tmp_dir_path, env.out_dir_path, vconf)
        for video_no in vconf.titleNoList:
            dl.download_one(video_no)
        print("end")

    def run_chzzk_live(self):
        disable_streamlink_log()
        url = f"https://chzzk.naver.com/{self.conf.chzzkLive.uid}"
        log.info(f"Start Record: {url}")
        if self.conf.chzzkLive.cookies:
            log.info("Using Credentials")
        req = self.conf.chzzkLive
        recorder = ChzzkLiveRecorder(
            req.uid, self.env.out_dir_path, req.cookies,
            self.create_amqp(), self.create_amqp(),
        )
        recorder.record()

    def run_soop_live(self):
        disable_streamlink_log()
        url = f"https://ch.sooplive.co.kr/{self.conf.soopLive.userId}"
        log.info(f"Start Record: {url}")
        if self.conf.soopLive.cred:
            log.info("Using Credentials")
        req = self.conf.soopLive
        recorder = SoopLiveRecorder(
            req.userId, self.env.out_dir_path, req.cred,
            self.create_amqp(), self.create_amqp(),
        )
        recorder.record()

    def run_twitch_live(self):
        disable_streamlink_log()
        url = f"https://www.twitch.tv/{self.conf.twitchLive.channelName}"
        log.info(f"Start Record: {url}")
        if self.conf.twitchLive.cookies:
            log.info("Using Credentials")
        req = self.conf.twitchLive
        recorder = TwitchLiveRecorder(
            req.channelName, self.env.out_dir_path, req.cookies,
            self.create_amqp(), self.create_amqp(),
        )
        recorder.record()

    def create_amqp(self):
        # return AmqpBlocking(self.env.amqp)
        if self.env.env == "prod":
            return AmqpBlocking(self.env.amqp)
        else:
            return AmqpMock()
