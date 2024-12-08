import asyncio
import json
import os
import time

from stdl.config.config import read_app_config_by_file, read_app_config_by_env
from stdl.config.env import get_env
from stdl.config.requests import RequestType
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.downloaders.ytdl.downloader import YtdlDownloader
from stdl.platforms.afreeca.recorder import AfreecaLiveRecorder
from stdl.platforms.afreeca.video_downloader import AfreecaVideoDownloader
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

        if self.conf.req_type() == RequestType.CHZZK_LIVE:
            self.run_chzzk_live()
        elif self.conf.req_type() == RequestType.CHZZK_VIDEO:
            self.run_chzzk_video()
        elif self.conf.req_type() == RequestType.AFREECA_LIVE:
            self.run_afreeca_live()
        elif self.conf.req_type() == RequestType.AFREECA_VIDEO:
            self.run_afreeca_video()
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

    def run_afreeca_video(self):
        env = self.env
        vconf = self.conf.afreecaVideo
        dl = AfreecaVideoDownloader(env.tmp_dir_path, env.out_dir_path, vconf)
        for video_no in vconf.titleNoList:
            dl.download_one(video_no)
        print("end")

    def run_chzzk_live(self):
        disable_streamlink_log()
        url = f"https://chzzk.naver.com/{self.conf.chzzkLive.uid}"
        log.info(f"Start record: {url}")
        log.info("Conf", self.conf.to_dict())
        req = self.conf.chzzkLive
        recorder = ChzzkLiveRecorder(
            req.uid, self.env.out_dir_path, self.env.tmp_dir_path,
            req.once, req.cookies,
        )
        recorder.record()

    def run_afreeca_live(self):
        disable_streamlink_log()
        url = f"https://ch.sooplive.co.kr/{self.conf.afreecaLive.userId}"
        log.info(f"Start record: {url}")
        log.info("Conf", self.conf.to_dict())
        req = self.conf.afreecaLive
        recorder = AfreecaLiveRecorder(
            req.userId, self.env.out_dir_path, self.env.tmp_dir_path,
            req.once, req.cred,
        )
        recorder.record()

    def run_twitch_live(self):
        disable_streamlink_log()
        log.info("Conf", self.conf.to_dict())
        req = self.conf.twitchLive
        recorder = TwitchLiveRecorder(
            req.channelName, self.env.out_dir_path, self.env.tmp_dir_path,
            req.once, req.cookies,
        )
        recorder.record()

