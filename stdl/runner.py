import asyncio
import json
import os
import time

from stdl.common.amqp import AmqpBlocking, AmqpMock
from stdl.common.request_config import read_app_config_by_file, read_app_config_by_env
from stdl.common.env import get_env
from stdl.common.request_types import RequestType
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.downloaders.ytdl.downloader import YtdlDownloader
from stdl.platforms.soop.recorder import SoopLiveRecorder
from stdl.platforms.soop.video_downloader import SoopVideoDownloader
from stdl.platforms.chzzk.recorder import ChzzkLiveRecorder
from stdl.platforms.chzzk.video_downloader import ChzzkVideoDownloader
from stdl.platforms.chzzk.video_downloader_legacy import ChzzkVideoDownloaderLegacy
from stdl.platforms.twitch.recorder import TwitchLiveRecorder
from stdl.utils.fs.fs_local import LocalFsAccessor
from stdl.utils.http import get_headers
from stdl.utils.logger import log
from stdl.utils.streamlink import disable_streamlink_log
from stdl.utils.url import get_query_string


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.conf = self.__read_config()
        self.ac = LocalFsAccessor()

    def __read_config(self):
        conf = read_app_config_by_env()
        if conf is None:
            conf_path = self.env.config_path
            if conf_path is not None:
                conf = read_app_config_by_file(conf_path)
        if conf is None:
            raise ValueError("Config not found")
        return conf

    def run(self):
        if self.conf.start_delay_ms > 0:
            log.info(f"Start delay: {self.conf.start_delay_ms}ms")
            time.sleep(self.conf.start_delay_ms / 1000)

        os.makedirs(self.env.out_dir_path, exist_ok=True)

        if self.conf.req_type == RequestType.CHZZK_LIVE:
            self.run_chzzk_live()
        elif self.conf.req_type == RequestType.CHZZK_VIDEO:
            self.run_chzzk_video()
        elif self.conf.req_type == RequestType.SOOP_LIVE:
            self.run_soop_live()
        elif self.conf.req_type == RequestType.SOOP_VIDEO:
            self.run_soop_video()
        elif self.conf.req_type == RequestType.TWITCH_LIVE:
            self.run_twitch_live()
        elif self.conf.req_type == RequestType.YTDL_VIDEO:
            self.run_ytdl_video()
        elif self.conf.req_type == RequestType.HLS_M3U8:
            self.run_hls_m3u8()
        else:
            raise ValueError("Invalid Request Type")

    def run_hls_m3u8(self):
        req = self.conf.hls_m3u8
        if req is None:
            raise ValueError("Invalid Request Type")
        if req.cookies is not None:
            headers = get_headers(json.loads(req.cookies))
        else:
            headers = get_headers()

        parallel_num = 30
        hls = HlsDownloader(
            self.env.tmp_dir_path,
            self.env.out_dir_path,
            headers,
            parallel_num,
        )
        for i, m3u8_url in enumerate(req.urls):
            qs = get_query_string(m3u8_url)
            title = f"hls_{i}"
            asyncio.run(hls.download_parallel(m3u8_url, "hls", title, qs))
        print("end")

    def run_ytdl_video(self):
        req = self.conf.youtube_video
        if req is None:
            raise ValueError("Invalid Request Type")
        yt = YtdlDownloader(self.env.out_dir_path)
        yt.download(req.urls)
        print("end")

    def run_chzzk_video(self):
        env = self.env
        req = self.conf.chzzk_video
        if req is None:
            raise ValueError("Invalid Request Type")
        dl = ChzzkVideoDownloader(env.tmp_dir_path, env.out_dir_path, req)
        dl_l = ChzzkVideoDownloaderLegacy(env.tmp_dir_path, env.out_dir_path, req)
        for video_no in req.video_no_list:
            try:
                dl.download_one(video_no)
            except TypeError:
                dl_l.download_one(video_no)
        print("end")

    def run_soop_video(self):
        env = self.env
        req = self.conf.soop_video
        if req is None:
            raise ValueError("Invalid Request Type")
        dl = SoopVideoDownloader(env.tmp_dir_path, env.out_dir_path, req)
        for video_no in req.title_no_list:
            dl.download_one(video_no)
        print("end")

    def run_chzzk_live(self):
        disable_streamlink_log()
        req = self.conf.chzzk_live
        if req is None:
            raise ValueError("Invalid Request Type")
        url = f"https://chzzk.naver.com/{req.uid}"
        log.info(f"Start Record: {url}")
        if req.cookies:
            log.info("Using Credentials")
        recorder = ChzzkLiveRecorder(
            req.uid,
            self.env.out_dir_path,
            req.cookies,
            self.ac,
            self.__create_amqp(),
            self.__create_amqp(),
        )
        recorder.record()

    def run_soop_live(self):
        disable_streamlink_log()
        req = self.conf.soop_live
        if req is None:
            raise ValueError("Invalid Request Type")
        url = f"https://ch.sooplive.co.kr/{req.user_id}"
        log.info(f"Start Record: {url}")
        if req.cred:
            log.info("Using Credentials")
        recorder = SoopLiveRecorder(
            req.user_id,
            self.env.out_dir_path,
            req.cred,
            self.ac,
            self.__create_amqp(),
            self.__create_amqp(),
        )
        recorder.record()

    def run_twitch_live(self):
        disable_streamlink_log()
        req = self.conf.twitch_live
        if req is None:
            raise ValueError("Invalid Request Type")
        url = f"https://www.twitch.tv/{req.channel_name}"
        log.info(f"Start Record: {url}")
        if req.cookies:
            log.info("Using Credentials")
        recorder = TwitchLiveRecorder(
            req.channel_name,
            self.env.out_dir_path,
            req.cookies,
            self.ac,
            self.__create_amqp(),
            self.__create_amqp(),
        )
        recorder.record()

    def __create_amqp(self):
        # return AmqpBlocking(self.env.amqp)
        if self.env.env == "prod":
            return AmqpBlocking(self.env.amqp)
        else:
            return AmqpMock()
