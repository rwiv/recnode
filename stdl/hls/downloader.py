import os
from typing import List, Optional
import asyncio
import aiohttp

from stdl.hls.utils import sub_lists_with_idx
from stdl.utils.logger import log

buf_size = 8192
retry_count = 3


class HttpError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP Error: {status_code}")


class HlsDownloader:
    def __init__(
            self,
            urls: List[str],
            base_dir_path: str,
            out_name: str,
            headers: Optional[dict] = None,
            parallel: Optional[int] = 30,
    ):
        self.urls = urls
        self.headers = headers
        self.base_dir_path = base_dir_path
        self.out_name = out_name.replace("?", "？").replace("/", "／")
        self.parallel = parallel

    async def download(self):
        subs = sub_lists_with_idx(self.urls, self.parallel)
        for sub in subs:
            log.info(f"{sub[0].idx}-{sub[0].idx + self.parallel}")
            dir_path = os.path.join(self.base_dir_path, self.out_name)
            os.makedirs(dir_path, exist_ok=True)

            tasks = [
                download_file_wraper(elem.value, self.headers, elem.idx, dir_path)
                for elem in sub
            ]
            await asyncio.gather(*tasks)


async def download_file_wraper(url: str, headers: Optional[dict[str, str]], num: int, out_dir_path: str):
    for i in range(retry_count):
        try:
            await download_file(url, headers, num, out_dir_path)
            break
        except HttpError as e:
            log.error(f"HTTP Error", {
                "retry": i,
                "error": e,
            })
    else:
        log.error(f"Failed to download {num + 1}.ts")


async def download_file(url: str, headers: Optional[dict[str, str]], num: int, out_dir_path: str):
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

