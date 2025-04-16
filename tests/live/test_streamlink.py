from urllib.parse import urlparse

import pytest
from streamlink.session.session import Streamlink
from streamlink.stream.hls.hls import HLSStream
from streamlink.stream.hls.m3u8 import M3U8Parser, M3U8
from streamlink.stream.hls.segment import HLSSegment

from stdl.utils import AsyncHttpClient

http = AsyncHttpClient()

url = ""


@pytest.mark.asyncio
async def test_streamlink_1():
    print()
    session = Streamlink()
    print(session.http.headers)
    streams = session.streams(url)
    stream: HLSStream = streams["best"]

    stream_url = stream.url
    headers = {}
    for k, v in session.http.headers.items():
        headers[k] = v

    text = await http.get_text(stream_url, headers)
    playlist: M3U8 = M3U8Parser().parse(text)
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

    print(base_url)
    b = await http.get_bytes("/".join([base_url, segments[0].uri]), headers=headers)
    # res = session.http.get("/".join([base_url, segments[0].uri]))
    # res = session.http.get(segments[0].uri)
    print(len(b) / 1024 / 1024)
