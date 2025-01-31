import asyncio
import json
import random

import requests

from stdl.common.requests import ChzzkVideoRequest
from stdl.platforms.chzzk.type_playback import ChzzkPlayback
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.utils.http import get_headers


class ChzzkVideoDownloader:

    def __init__(self, tmp_dir: str, out_dir: str, req: ChzzkVideoRequest):
        self.cookies = None
        if req.cookies is not None:
            self.cookies = json.loads(req.cookies)
        self.req = req
        self.hls = HlsDownloader(
            tmp_dir, out_dir, get_headers(self.cookies),
            req.parallelNum, req.nonParallelDelayMs,
        )

    def download_one(self, video_no: int):
        m3u8_url, title, channelId = self._get_info(video_no)
        rand_num = random.randint(100000, 999999)
        file_title = f"{rand_num}_{title}"
        if self.req.isParallel:
            asyncio.run(self.hls.download_parallel(m3u8_url, channelId, file_title))
        else:
            asyncio.run(self.hls.download_non_parallel(m3u8_url, channelId, file_title))

    def _get_info(self, video_no: int) -> tuple[str, str, str]:
        res = self._request_video_info(video_no)
        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        pb = ChzzkPlayback(**json.loads(res["content"]["liveRewindPlaybackJson"]))
        if len(pb.media) != 1:
            raise ValueError("media should be 1")

        m3u8_url = pb.media[0].path
        return m3u8_url, title, channelId

    def _request_video_info(self, video_no: int) -> dict[str, any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.cookies, "application/json"))
        return res.json()
