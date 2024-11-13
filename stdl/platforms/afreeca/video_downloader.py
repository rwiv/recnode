import asyncio
import json
import os
import random
import shutil
import subprocess
from os.path import join

import requests

from stdl.config.requests import AfreecaVideoRequest
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.platforms.afreeca.afreeca_hls_url_extractor import AfreecaHlsUrlExtractor
from stdl.utils.file import write_file, sanitize_filename
from stdl.utils.http import get_headers


class AfreecaVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, req: AfreecaVideoRequest):
        self.cookies = None
        if req.cookies is not None:
            self.cookies = json.loads(req.cookies)
        self.req = req
        self.out_dir = out_dir
        self.tmp_dir = tmp_dir
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
        os.makedirs(join(self.out_dir, bjId), exist_ok=True)
        os.makedirs(join(self.tmp_dir, bjId), exist_ok=True)
        file_title = sanitize_filename(title)
        out_paths = [join(self.out_dir, bjId, f"{file_title}_{i}.mp4") for i in range(len(m3u8_urls))]
        tmp_paths = [join(self.tmp_dir, bjId, f"{i}.mp4") for i in range(len(m3u8_urls))]
        for i, out_path in enumerate(out_paths):
            shutil.move(out_path, tmp_paths[i])

        list_path = join(self.tmp_dir, bjId, "list.txt")
        write_file(list_path, "\n".join([f"file '{f}'" for f in tmp_paths]))
        rand_num = random.randint(100000, 999999)
        out_path = join(self.tmp_dir, bjId, f"{rand_num}_{file_title}.mp4")
        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", out_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        shutil.move(out_path, out_path.replace(self.tmp_dir, self.out_dir))
        os.remove(list_path)
        for input_file in tmp_paths:
            os.remove(input_file)
        print(title)

    def _get_url(self, title_no: int):
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(url, headers=get_headers(self.cookies, "application/json"), data={
            "nTitleNo": title_no, "nApiLevel": 10, "nPlaylistIdx": 0,
        }).json()
        data = res["data"]
        return [f["file"] for f in data["files"]], data["full_title"], data["bj_id"]
