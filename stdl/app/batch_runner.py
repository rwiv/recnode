import asyncio
import logging
import uuid
from datetime import datetime

import yaml
from pydantic import BaseModel
from pyutils import log
from streamlink.stream.hls.hls import HLSStream

from ..common.env import get_env
from ..data.live import LiveState
from ..fetcher import PlatformFetcher
from ..file import create_fs_writer
from ..recorder import RecorderResolver, disable_streamlink_log, StreamLinkSessionArgs, get_streams


class BatchConfig(BaseModel):
    url: str
    cookie: str | None = None


def read_conf(config_path: str) -> BatchConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return BatchConfig(**yaml.load(text, Loader=yaml.FullLoader))


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.writer = create_fs_writer(self.env)
        self.recorder_resolver = RecorderResolver(self.env, self.writer)

    def run(self):
        log.set_level(logging.DEBUG)
        disable_streamlink_log()

        if self.env.config_path is None:
            raise ValueError("Config path not set")
        conf = read_conf(self.env.config_path)

        state = get_state(conf)
        recorder = self.recorder_resolver.create_recorder(state=state)
        recorder.record(state=state, block=True)


def get_state(conf: BatchConfig):
    streams = get_streams(url=conf.url, args=StreamLinkSessionArgs(cookie_header=conf.cookie))
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
    if conf.cookie is not None:
        headers["Cookie"] = conf.cookie

    fetcher = PlatformFetcher()
    if len(fetcher.headers) == 0:
        fetcher.set_headers(headers)

    live = asyncio.run(fetcher.fetch_live_info(conf.url))
    if live is None:
        raise ValueError("Channel not live")

    return LiveState(
        id=str(uuid.uuid4()),
        platform=live.platform,
        channelId=live.channel_id,
        channelName=live.channel_name,
        liveId=live.live_id,
        liveTitle=live.live_title,
        streamUrl=stream_url,
        headers=headers,
        videoName=datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
