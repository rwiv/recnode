import asyncio
import json
import os

from pyutils import get_query_string

from ..common.env import get_env
from ..common.fs import create_fs_writer
from ..common.request import read_request_by_env, RequestType
from ..record import RecorderResolver, disable_streamlink_log
from ..utils.hls.downloader import HlsDownloader
from ..utils.http import get_headers
from ..utils.ytdl.ytdl_downloader import YtdlDownloader
from ..video import ChzzkVideoDownloader, ChzzkVideoDownloaderLegacy, SoopVideoDownloader


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.conf = read_request_by_env(self.env)
        self.writer = create_fs_writer(self.env.fs_name, self.env.fs_config_path)
        self.recorder_resolver = RecorderResolver(self.env, self.conf, self.writer)

    def run(self):
        if self.conf.req_type in {RequestType.CHZZK_LIVE, RequestType.SOOP_LIVE, RequestType.TWITCH_LIVE}:
            return self.__record_live()

        os.makedirs(self.env.tmp_dir_path, exist_ok=True)  # TODO: update
        if self.conf.req_type == RequestType.CHZZK_VIDEO:
            return self.__run_chzzk_video()
        elif self.conf.req_type == RequestType.SOOP_VIDEO:
            return self.__run_soop_video()
        elif self.conf.req_type == RequestType.YTDL_VIDEO:
            return self.__run_ytdl_video()
        elif self.conf.req_type == RequestType.HLS_M3U8:
            return self.__run_hls_m3u8()

        raise ValueError("Invalid Request Type")

    def __run_hls_m3u8(self):
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

    def __run_ytdl_video(self):
        req = self.conf.youtube_video
        if req is None:
            raise ValueError("Invalid Request Type")
        yt = YtdlDownloader(self.env.out_dir_path)
        yt.download(req.urls)
        print("end")

    def __run_chzzk_video(self):
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

    def __run_soop_video(self):
        env = self.env
        req = self.conf.soop_video
        if req is None:
            raise ValueError("Invalid Request Type")
        dl = SoopVideoDownloader(env.tmp_dir_path, env.out_dir_path, req)
        for video_no in req.title_no_list:
            dl.download_one(video_no)
        print("end")

    def __record_live(self):
        disable_streamlink_log()
        recorder = self.recorder_resolver.create_recorder()
        recorder.record(block=True)
