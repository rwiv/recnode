from urllib.parse import urlparse

from requests import Response
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream
from streamlink.stream.hls.m3u8 import M3U8Parser
from streamlink.stream.hls.segment import HLSSegment

url = ""


def test_streamlink_1():
    print()
    session = Streamlink()
    print(session.http.headers)
    streams = session.streams(url)
    stream: HLSStream = streams["best"]

    stream_url = stream.url
    res: Response = session.http.get(stream_url)

    playlist = M3U8Parser().parse(res.text)
    if playlist.is_master:
        raise ValueError("Expected a media playlist, got a master playlist")

    segments: list[HLSSegment] = playlist.segments
    for seg in segments:
        print(seg)

    base_url = "/".join(stream_url.split("/")[:-1])
    parsed_url = urlparse(stream_url)
    query_string = parsed_url.query

    print(stream_url)
    print(query_string)

    res = session.http.get("/".join([base_url, segments[0].uri]))
    # res = session.http.get(segments[0].uri)
    print(len(res.content) / 1024 / 1024)
