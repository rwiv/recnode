import asyncio
import json
from typing import Optional

import requests
from dacite import from_dict

from stdl.platforms.chzzk.type_playback import ChzzkPlayback
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.utils.http import get_headers


class ChzzkVideoDownloader:

    def __init__(self, tmp_dir: str, out_dir: str, parallel: bool = False, cookie_str: Optional[str] = None):
        if cookie_str is None:
            raise ValueError("cookie_str is required")
        self.cookies = json.loads(cookie_str)
        self.parallel = parallel
        self.hls = HlsDownloader(tmp_dir, out_dir, get_headers(self.cookies))

    def download_one(self, video_no: int):
        m3u8_url, title, channelId = self._get_info(video_no)
        if self.parallel:
            asyncio.run(self.hls.download_parallel(m3u8_url, channelId, title))
        else:
            asyncio.run(self.hls.download_non_parallel(m3u8_url, channelId, title))

    def _get_info(self, video_no: int) -> tuple[str, str, str]:
        res = self._request_video_info(video_no)
        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        pb = from_dict(data_class=ChzzkPlayback, data=json.loads(res["content"]["liveRewindPlaybackJson"]))
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        m3u8_url = pb.media[0].path
        return m3u8_url, title, channelId

    def _request_video_info(self, video_no: int) -> dict[str, any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.cookies, "application/json"))
        return res.json()
