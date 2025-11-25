import uuid
from datetime import datetime

import yaml
from pydantic import BaseModel
from pyutils import log, parse_query_params
from streamlink.stream.hls.hls import HLSStream

from ..data.live import LiveState, LocationType
from ..fetcher import PlatformFetcher
from ..utils import StreamLinkSessionArgs, get_streams, AsyncHttpClient, FIREFOX_USER_AGENT


async def get_live_state(live_url: str, fs_name: str, platform_cookie: str | None, stream_params_str: str | None):
    fetcher = PlatformFetcher(AsyncHttpClient())
    if "User-Agent" not in fetcher.headers:
        fetcher.headers["User-Agent"] = FIREFOX_USER_AGENT
    if platform_cookie is not None:
        fetcher.headers["Cookie"] = platform_cookie

    streams = get_streams(url=live_url, args=StreamLinkSessionArgs(cookie_header=platform_cookie))
    if streams is None:
        log.error("Failed to get live streams")
        raise ValueError("Failed to get live streams")

    stream: HLSStream | None = streams.get("best")
    if stream is None:
        raise ValueError("Failed to get best stream")

    # Set http session context
    stream_headers = {}
    for k, v in stream.session.http.headers.items():
        stream_headers[k] = v

    stream_params = None
    if stream_params_str is not None:
        stream_params = parse_query_params(stream_params_str)

    live_info = await fetcher.fetch_live_info(live_url)
    if live_info is None:
        raise ValueError("Channel not live")

    now = datetime.now()
    return LiveState(
        id=str(uuid.uuid4()),
        platform=live_info.platform,
        channelId=live_info.channel_id,
        channelName=live_info.channel_name,
        liveId=live_info.live_id,
        liveTitle=live_info.live_title,
        streamUrl=stream.url,
        streamHeaders=stream_headers,
        streamParams=stream_params,
        platformCookie=platform_cookie,
        videoName=now.strftime("%Y%m%d_%H%M%S"),
        fsName=fs_name,
        location=LocationType.LOCAL,
        isInvalid=False,
        createdAt=now,
        updatedAt=now,
    )


class BatchConfig(BaseModel):
    url: str
    params: str | None = None
    cookie: str | None = None
    fs_name: str


def read_conf(config_path: str) -> BatchConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return BatchConfig(**yaml.load(text, Loader=yaml.FullLoader))
