import asyncio
import json
import os
import subprocess
from os.path import join

import requests

from stdl.config.requests import AfreecaVideoRequest
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.platforms.afreeca.afreeca_hls_url_extractor import AfreecaHlsUrlExtractor
from stdl.utils.file import write_file
from stdl.utils.http import get_headers


class AfreecaVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, req: AfreecaVideoRequest):
        self.cookies = None
        if req.cookies is not None:
            self.cookies = json.loads(req.cookies)
        self.req = req
        self.out_dir = out_dir
        self.hls = HlsDownloader(
            tmp_dir, out_dir, get_headers(self.cookies),
            req.parallelNum, req.nonParallelDelayMs,
            AfreecaHlsUrlExtractor(),
        )

    def download_one(self, title_no: int):
        m3u8_urls, title, bjId = self._get_url(title_no)
        for i, m3u8_url in enumerate(m3u8_urls):
            if self.req.isParallel:
                asyncio.run(self.hls.download_parallel(m3u8_url, bjId, f"{title}_{i}"))
            else:
                asyncio.run(self.hls.download_non_parallel(m3u8_url, bjId, f"{title}_{i}"))

        # merge
        base_path = join(self.out_dir, bjId)
        out_paths = [join(base_path, f"{title}_{i}.mp4") for i in range(len(m3u8_urls))]
        new_paths = [join(base_path, f"{i}.mp4") for i in range(len(m3u8_urls))]
        for i, out_path in enumerate(out_paths):
            os.rename(out_path, new_paths[i])

        list_path = f"{join(base_path, "list")}.txt"
        write_file(list_path, "\n".join([f"file '{f}'" for f in new_paths]))
        out_path = join(base_path, f"{title}.mp4")
        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", out_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        os.remove(list_path)
        for input_file in new_paths:
            os.remove(input_file)
        print(title)

    def _get_url(self, title_no: int):
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(url, headers=get_headers(self.cookies, "application/json"), data={
            "nTitleNo": title_no, "nApiLevel": 10, "nPlaylistIdx": 0,
        }).json()
        data = res["data"]
        return [f["file"] for f in data["files"]], data["full_title"], data["bj_id"]
