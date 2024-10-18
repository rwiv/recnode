import asyncio
import json
import os

from stdl.config.config import read_app_config_by_file, read_app_config_by_env
from stdl.config.env import get_env
from stdl.config.requests import RequestType
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.downloaders.ytdl.downloader import YtdlDownloader
from stdl.platforms.afreeca.recorder import AfreecaLiveRecorder
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
        os.makedirs(self.env.out_dir_path, exist_ok=True)
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
        hls = HlsDownloader(base_dir_path=self.env.out_dir_path, headers=headers)
        for i, m3u8_url in enumerate(req.urls):
            qs = get_query_string(m3u8_url)
            title = f"hls_{i}"
            asyncio.run(hls.download(m3u8_url, title, qs))
        print("end")

    def run_ytdl_video(self):
        yt = YtdlDownloader(self.env.out_dir_path)
        yt.download(self.conf.youtubeVideo.urls)
        print("end")

    def run_chzzk_video(self):
        dl = ChzzkVideoDownloader(self.env.out_dir_path, self.conf.chzzkVideo.cookies)
        dl_l = ChzzkVideoDownloaderLegacy(self.env.out_dir_path, self.conf.chzzkVideo.cookies)
        try:
            dl.download(self.conf.chzzkVideo.videoNoList)
        except TypeError:
            dl_l.download(self.conf.chzzkVideo.videoNoList)
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
        log.info("Conf", self.conf.to_dict())
        req = self.conf.afreecaLive
        recorder = AfreecaLiveRecorder(
            req.userId, self.env.out_dir_path, self.env.tmp_dir_path,
            req.once, self.env.afreeca_credential,
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
