import asyncio
import os
import time
from typing import List, Optional

import aiohttp
import requests

from stdl.downloaders.hls.parser import parse_master_playlist, parse_media_playlist
from stdl.downloaders.hls.utils import sub_lists_with_idx
from stdl.downloaders.hls.merge import merge_hls_chunks
from stdl.utils.file import sanitize_filename
from stdl.utils.logger import log
from stdl.utils.url import get_base_url

buf_size = 8192
retry_count = 5


class HttpError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP Error: {status_code}")


class HlsDownloader:
    def __init__(
            self,
            tmp_dir_path: str,
            out_dir_path: str,
            headers: Optional[dict] = None,
            # parallel: Optional[int] = 50,
            parallel: Optional[int] = 10,
    ):
        self.headers = headers
        self.tmp_dir_path = tmp_dir_path
        self.out_dir_path = out_dir_path
        self.parallel = parallel

    async def download(
            self, m3u8_url: str, name: str, title: str,
            qs: Optional[str] = None,
    ):
        title_name = sanitize_filename(title)
        dir_path = os.path.join(self.tmp_dir_path, name, title_name)
        urls = _get_urls(m3u8_url, qs)
        subs = sub_lists_with_idx(urls, self.parallel)
        # TODO remove
        subs = [subs[len(subs)-1]]
        for sub in subs:
            log.info(f"{sub[0].idx}-{sub[0].idx + self.parallel}")
            os.makedirs(dir_path, exist_ok=True)

            tasks = [
                _download_file_wrapper(elem.value, self.headers, elem.idx, dir_path)
                for elem in sub
            ]
            await asyncio.gather(*tasks)

        merge_hls_chunks(dir_path, self.out_dir_path, name)


def _get_urls(m3u8_url: str, qs: Optional[str] = None) -> List[str]:
    m3u8 = requests.get(m3u8_url).text
    pl = parse_master_playlist(m3u8)

    if len(pl.resolutions) == 0:
        raise ValueError("No resolutions found")

    r = pl.resolutions[0]
    for cur in pl.resolutions:
        if cur.resolution > r.resolution:
            r = cur

    base_url = f"{get_base_url(m3u8_url)}/{r.name}"
    if qs is not None and qs != "":
        base_url += f"?{qs}"
    m3u8 = requests.get(base_url).text
    pl = parse_media_playlist(m3u8, get_base_url(base_url), qs)
    return pl.segment_paths


async def _download_file_wrapper(url: str, headers: Optional[dict[str, str]], num: int, out_dir_path: str):
    for i in range(retry_count):
        try:
            await _download_file(url, headers, num, out_dir_path)
            break
        except Exception as e:
            print(f"HTTP Error: cnt={i}, error={e}")
            time.sleep(1)
    else:
        # raise Exception(f"Failed to download, cnt={num + 1}")
        print(f"Failed to download, cnt={num + 1}")


async def _download_file(url: str, headers: Optional[dict[str, str]], num: int, out_dir_path: str):
    file_path = os.path.join(out_dir_path, f"{num + 1}.ts")
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as res:
            if res.status >= 400:
                raise HttpError(res.status)
            with open(file_path, 'wb') as file:
                while True:
                    chunk = await res.content.read(buf_size)
                    if not chunk:
                        break
                    file.write(chunk)
