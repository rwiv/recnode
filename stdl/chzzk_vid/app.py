import requests
from xml.etree.ElementTree import fromstring, Element
from stdl.chzzk_vid.type_video import ChzzkVideoResponse
from stdl.utils.url import find_query_value_one, get_base_url
from dacite import from_dict

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


def parse_xml(xml_str) -> Element:
    return fromstring(xml_str)


def run(videoNo: int):
    res = request_video_info(videoNo)
    videoId = res.content.videoId
    key = res.content.inKey
    m3u_url, lsu_sa, base_url = request_play_info(videoId, key)
    print(m3u_url)
    print(lsu_sa)
    print(base_url)


def request_video_info(videoNo: int) -> ChzzkVideoResponse:
    url = f"https://api.chzzk.naver.com/service/v3/videos/{videoNo}"
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    json = requests.get(url, headers=headers).json()
    return from_dict(data_class=ChzzkVideoResponse, data=json)


def request_play_info(videoId: str, key: str):
    url = f"https://apis.naver.com/neonplayer/vodplay/v1/playback/{videoId}?key={key}"
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/xml",
    }
    res = requests.get(url, headers=headers).text
    root = parse_xml(res)
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
