import os
from dataclasses import dataclass
from typing import List, Optional
import asyncio
import aiohttp

from stdl.hls.utils import sub_lists_with_idx
from stdl.utils.logger import log


class HttpError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP Error: {status_code}")


@dataclass
class HlsDownloaderArgs:
    urls: List[str]
    headers: dict
    base_dir_path: str
    out_name: str
    parallel: Optional[int] = 10


class HlsDownloader:
    def __init__(
            self,
            urls: List[str],
            base_dir_path: str,
            out_name: str,
            headers: Optional[dict] = None,
            parallel: Optional[int] = 10,
    ):
        self.urls = urls
        self.headers = headers
        self.base_dir_path = base_dir_path
        self.out_name = out_name
        self.parallel = parallel

    async def download(self):
        subs = sub_lists_with_idx(self.urls, self.parallel)
        for sub in subs:
            log.info(f"{sub[0].idx}-{sub[0].idx + self.parallel}")
            dir_path = os.path.join(self.base_dir_path, self.out_name)
            os.makedirs(dir_path, exist_ok=True)

            tasks = [
                download_file(elem.value, self.headers, elem.idx, dir_path)
                for elem in sub
            ]
            await asyncio.gather(*tasks)


async def download_file(url: str, headers: Optional[dict], num: int, out_dir_path: str):
    buf_size = 8192
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

