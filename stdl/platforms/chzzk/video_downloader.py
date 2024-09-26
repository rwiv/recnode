import asyncio
import json

import requests
from dacite import from_dict

from stdl.platforms.chzzk.type_playback import ChzzkPlayback
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.utils.http import get_headers


class ChzzkVideoDownloader:

    def __init__(self, out_dir: str, cookie_str: str = None):
        self.cookies = None
        if cookie_str is not None:
            self.cookies = json.loads(cookie_str)
        self.hls = HlsDownloader(base_dir_path=out_dir, headers=get_headers(self.cookies))

    def download(self, video_no_list: list[int]):
        for video_no in video_no_list:
            self._download_one(video_no)

    def _download_one(self, video_no: int):
        m3u8_url, title = self._get_info(video_no)
        asyncio.run(self.hls.download(m3u8_url, title))

    def _get_info(self, video_no: int):
        res = self._request_video_info(video_no)
        title = res["content"]["videoTitle"]
        pb = from_dict(data_class=ChzzkPlayback, data=json.loads(res["content"]["liveRewindPlaybackJson"]))
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        m3u8_url = pb.media[0].path
        return m3u8_url, title

    def _request_video_info(self, video_no: int) -> dict[str, any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.cookies, "application/json"))
        return res.json()

