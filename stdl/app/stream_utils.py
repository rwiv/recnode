import uuid
from datetime import datetime

import yaml
from pydantic import BaseModel
from pyutils import log
from streamlink.stream.hls.hls import HLSStream

from ..data.live import LiveState, LocationType
from ..fetcher import PlatformFetcher
from ..utils import StreamLinkSessionArgs, get_streams, AsyncHttpClient


async def get_live_state(url: str, cookie_header: str | None):
    streams = get_streams(url=url, args=StreamLinkSessionArgs(cookie_header=cookie_header))
    if streams is None:
        log.error("Failed to get live streams")
        raise ValueError("Failed to get live streams")

    stream: HLSStream | None = streams.get("best")
    if stream is None:
        raise ValueError("Failed to get best stream")

    # Set http session context
    stream_url = stream.url
    headers = {}
    for k, v in stream.session.http.headers.items():
        headers[k] = v

    fetcher = PlatformFetcher(AsyncHttpClient())
    if len(fetcher.headers) == 0:
        fetcher.set_headers(headers)

    live_info = await fetcher.fetch_live_info(url)
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
        streamUrl=stream_url,
        headers=headers,
        platformCookie=cookie_header,
        videoName=now.strftime("%Y%m%d_%H%M%S"),
        location=LocationType.LOCAL,
        isInvalid=False,
        createdAt=now,
        updatedAt=now,
    )


class BatchConfig(BaseModel):
    url: str
    cookie: str | None = None


def read_conf(config_path: str) -> BatchConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return BatchConfig(**yaml.load(text, Loader=yaml.FullLoader))
