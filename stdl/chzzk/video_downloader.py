import asyncio
import json
from typing import Optional

import requests
from dacite import from_dict

from stdl.chzzk.type_playback import ChzzkPlayback
from stdl.chzzk.type_video import ChzzkVideoResponse
from stdl.hls.downloader import HlsDownloader
from stdl.hls.parser import parse_master_playlist, parse_media_playlist
from stdl.utils.http import create_cookie_str
from stdl.utils.url import get_base_url

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


class ChzzkVideoDownloader:

    def __init__(
            self,
            out_dir: str,
            cookie_str: str = None,
    ):
        self.out_dir = out_dir
        self.cookies = None
        if cookie_str is not None:
            self.cookies = json.loads(cookie_str)

    def download(self, video_no_list: list[int]):
        for video_no in video_no_list:
            self._download_one(video_no)

    def _download_one(self, video_no: int):
        urls, title = self._get_urls(video_no)
        dl = HlsDownloader(
            urls=urls,
            base_dir_path=self.out_dir,
            out_name=title,
            headers=self._get_headers(),
        )
        asyncio.run(dl.download())

    def _get_urls(self, video_no: int):
        res = self._request_video_info(video_no)
        title = res.content.videoTitle
        pb = from_dict(data_class=ChzzkPlayback, data=json.loads(res.content.liveRewindPlaybackJson))
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        m3u8_url = pb.media[0].path
        m3u8 = requests.get(m3u8_url).text
        pl = parse_master_playlist(m3u8)

        if len(pl.resolutions) == 0:
            raise ValueError("No resolutions found")

        r = pl.resolutions[0]
        for cur in pl.resolutions:
            if cur.resolution > r.resolution:
                r = cur

        base_url = f"{get_base_url(m3u8_url)}/{r.name}"
        m3u8 = requests.get(base_url).text
        pl = parse_media_playlist(m3u8, get_base_url(base_url))
        return pl.segment_paths, title

    def _get_headers(self, accept: Optional[str] = None) -> dict:
        headers = {
            "User-Agent": user_agent,
        }
        if accept is not None:
            headers["Accept"] = accept
        if self.cookies is not None:
            headers["Cookie"] = create_cookie_str(self.cookies)
        return headers

    def _request_video_info(self, video_no: int) -> ChzzkVideoResponse:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=self._get_headers("application/json"))
        return from_dict(data_class=ChzzkVideoResponse, data=res.json())

