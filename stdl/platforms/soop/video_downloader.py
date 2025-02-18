import asyncio
import json
import os
import random
import shutil
import subprocess

import requests

from stdl.common.request_types import SoopVideoRequest
from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.platforms.soop.hls_url_extractor import SoopHlsUrlExtractor
from stdl.utils.file import write_file, sanitize_filename
from stdl.utils.http import get_headers
from stdl.utils.path import path_join


class SoopVideoDownloader:
    def __init__(self, tmp_dir: str, out_dir: str, req: SoopVideoRequest):
        self.cookies = None
        if req.cookies is not None:
            self.cookies = json.loads(req.cookies)
        self.req = req
        self.out_dir = out_dir
        self.tmp_dir = tmp_dir
        self.hls = HlsDownloader(
            tmp_dir,
            out_dir,
            get_headers(self.cookies),
            req.parallel_num,
            req.non_parallel_delay_ms,
            SoopHlsUrlExtractor(),
        )

    def download_one(self, title_no: int):
        m3u8_urls, title, bjId = self._get_url(title_no)
        for i, m3u8_url in enumerate(m3u8_urls):
            if self.req.is_parallel:
                asyncio.run(self.hls.download_parallel(m3u8_url, bjId, f"{title}_{i}"))
            else:
                asyncio.run(self.hls.download_non_parallel(m3u8_url, bjId, f"{title}_{i}"))

        # merge
        os.makedirs(path_join(self.out_dir, bjId), exist_ok=True)
        os.makedirs(path_join(self.tmp_dir, bjId), exist_ok=True)
        file_title = sanitize_filename(title)
        out_paths = [path_join(self.out_dir, bjId, f"{file_title}_{i}.mp4") for i in range(len(m3u8_urls))]
        tmp_paths = [path_join(self.tmp_dir, bjId, f"{i}.mp4") for i in range(len(m3u8_urls))]
        for i, out_path in enumerate(out_paths):
            shutil.move(out_path, tmp_paths[i])

        list_path = path_join(self.tmp_dir, bjId, "list.txt")
        write_file(list_path, "\n".join([f"file '{f}'" for f in tmp_paths]))
        rand_num = random.randint(100000, 999999)
        out_path = path_join(self.tmp_dir, bjId, f"{rand_num}_{file_title}.mp4")
        command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", out_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        shutil.move(out_path, out_path.replace(self.tmp_dir, self.out_dir))
        os.remove(list_path)
        for input_file in tmp_paths:
            os.remove(input_file)
        print(title)

    def _get_url(self, title_no: int):
        url = f"https://api.m.sooplive.co.kr/station/video/a/view"
        res = requests.post(
            url,
            headers=get_headers(self.cookies, "application/json"),
            data={
                "nTitleNo": title_no,
                "nApiLevel": 10,
                "nPlaylistIdx": 0,
            },
        ).json()
        data = res["data"]
        return [f["file"] for f in data["files"]], data["full_title"].strip(), data["bj_id"]
