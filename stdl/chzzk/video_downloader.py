import json
import asyncio
from typing import Optional
from xml.etree.ElementTree import fromstring, Element

import requests
from dacite import from_dict

from stdl.hls.downloader import HlsDownloader
from stdl.chzzk.type_video import ChzzkVideoResponse
from stdl.hls.parser import parse_master_playlist, parse_media_playlist
from stdl.utils.http import create_cookie_str
from stdl.utils.url import find_query_value_one, get_base_url

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

    def _get_headers(self, accept: Optional[str] = None) -> dict:
        headers = {
            "User-Agent": user_agent,
        }
        if accept is not None:
            headers["Accept"] = accept
        if self.cookies is not None:
            headers["Cookie"] = create_cookie_str(self.cookies)
        return headers

    def _get_urls(self, video_no: int):
        res = self._request_video_info(video_no)
        title = res.content.videoTitle
        videoId = res.content.videoId
        key = res.content.inKey
        m3u_url, lsu_sa, base_url = self._request_play_info(videoId, key)
        res = requests.get(m3u_url)
        pl = parse_master_playlist(res.text)

        if len(pl.resolutions) == 0:
            raise ValueError("No resolutions found")

        r = pl.resolutions[0]
        for cur in pl.resolutions:
            if cur.resolution > r.resolution:
                r = cur

        m3u_url = f"{base_url}/{r.name}?_lsu_sa_={lsu_sa}"
        res = requests.get(m3u_url)
        pl = parse_media_playlist(res.text, base_url)
        urls = []
        for seg in pl.segment_paths:
            urls.append(f"{seg}?_lsu_sa_={lsu_sa}")
        return urls, title

    def _request_video_info(self, video_no: int) -> ChzzkVideoResponse:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=self._get_headers("application/json"))
        return from_dict(data_class=ChzzkVideoResponse, data=res.json())

    def _request_play_info(self, video_id: str, key: str):
        url = f"https://apis.naver.com/neonplayer/vodplay/v1/playback/{video_id}?key={key}"
        res = requests.get(url, headers=self._get_headers("application/xml")).text
        root = _parse_xml(res)
        if len(root) != 1:
            raise ValueError("root element should be 1")
        period = root[0]
        m3u_url = ""
        for child in period:
            attr = child.attrib
            if attr["mimeType"] == "video/mp2t":
                target_key = ""
                for key in attr.keys():
                    if key.endswith("m3u"):
                        target_key = key
                        break
                if target_key == "":
                    raise ValueError("target key not found")
                m3u_url = attr[target_key]
                break
        if m3u_url == "":
            raise ValueError("m3u_url not found")

        return m3u_url, find_query_value_one(m3u_url, "_lsu_sa_"), get_base_url(m3u_url)


def _parse_xml(xml_str) -> Element:
    return fromstring(xml_str)

