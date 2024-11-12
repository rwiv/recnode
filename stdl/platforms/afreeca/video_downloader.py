import asyncio
import json

import requests

from stdl.config.requests import AfreecaVideoRequest
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.platforms.afreeca.afreeca_hls_url_extractor import AfreecaHlsUrlExtractor
from stdl.utils.http import get_headers


class AfreecaVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, req: AfreecaVideoRequest):
        self.cookies = None
        if req.cookies is not None:
            self.cookies = json.loads(req.cookies)
        self.req = req
        self.hls = HlsDownloader(
            tmp_dir, out_dir, get_headers(self.cookies),
            req.parallelNum, req.nonParallelDelayMs,
            AfreecaHlsUrlExtractor(),
        )

    def download_one(self, title_no: int):
        m3u8_url, title, bjId = self._get_url(title_no)
        if self.req.isParallel:
            asyncio.run(self.hls.download_parallel(m3u8_url, bjId, title))
        else:
            asyncio.run(self.hls.download_non_parallel(m3u8_url, bjId, title))
        print(m3u8_url)

    def _get_url(self, title_no: int):
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(url, headers=get_headers(self.cookies, "application/json"), data={
            "nTitleNo": title_no, "nApiLevel": 10, "nPlaylistIdx": 0,
        }).json()
        data = res["data"]
        files = data["files"]
        if len(files) != 1:
            raise ValueError("files should be 1")
        return files[0]["file"], data["full_title"], data["bj_id"]
