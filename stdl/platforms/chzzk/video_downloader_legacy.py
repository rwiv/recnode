import asyncio
import json
from xml.etree.ElementTree import fromstring, Element

import requests

from stdl.downloaders.hls.downloader import HlsDownloader
from stdl.utils.url import find_query_value_one, get_base_url
from stdl.utils.http import get_headers


class ChzzkVideoDownloaderLegacy:

    def __init__(self, tmp_dir: str, out_dir: str, cookie_str: str = None):
        self.cookies = None
        if cookie_str is not None:
            self.cookies = json.loads(cookie_str)
        self.hls = HlsDownloader(tmp_dir, out_dir, get_headers(self.cookies))

    def download_one(self, video_no: int):
        m3u_url, qs, title, channelId = self._get_info(video_no)
        asyncio.run(self.hls.download_non_parallel(m3u_url, channelId, title, qs))

    def _get_info(self, video_no: int) -> tuple[str, str, str, str]:
        res = self._request_video_info(video_no)
        channelId = res["content"]["channel"]["channelId"]
        title = res["content"]["videoTitle"]
        videoId = res["content"]["videoId"]
        key = res["content"]["inKey"]
        m3u_url, lsu_sa, base_url = self._request_play_info(videoId, key)
        qs = f"_lsu_sa_={lsu_sa}"
        return m3u_url, qs, title, channelId

    def _request_video_info(self, video_no: int) -> dict[str, any]:
        url = f"https://api.chzzk.naver.com/service/v3/videos/{video_no}"
        res = requests.get(url, headers=get_headers(self.cookies, "application/json"))
        return res.json()

    def _request_play_info(self, video_id: str, key: str):
        url = f"https://apis.naver.com/neonplayer/vodplay/v1/playback/{video_id}?key={key}"
        res = requests.get(url, headers=get_headers(self.cookies, "application/xml")).text
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

        lsu_sa = find_query_value_one(m3u_url, "_lsu_sa_")
        base_url = get_base_url(m3u_url)
        return m3u_url, lsu_sa, base_url


def _parse_xml(xml_str) -> Element:
    return fromstring(xml_str)
