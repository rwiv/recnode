import asyncio
import logging
import uuid
from datetime import datetime

import yaml
from pydantic import BaseModel
from pyutils import log
from redis.asyncio import Redis
from streamlink.stream.hls.hls import HLSStream

from ..config import get_env
from ..data.live import LiveState, LiveStateService
from ..data.redis import create_redis_pool
from ..fetcher import PlatformFetcher
from ..file import create_fs_writer
from ..metric import MetricManager
from ..recorder import RecorderResolver, disable_streamlink_log, StreamLinkSessionArgs, get_streams


class BatchConfig(BaseModel):
    url: str
    cookie: str | None = None


def read_conf(config_path: str) -> BatchConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return BatchConfig(**yaml.load(text, Loader=yaml.FullLoader))


async def async_input(prompt: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.metric = MetricManager()
        self.writer = create_fs_writer(self.env, self.metric)
        self.fetcher = PlatformFetcher(self.metric)
        self.redis_pool = create_redis_pool(self.env.redis)
        self.redis_client = Redis(connection_pool=self.redis_pool)
        self.recorder_resolver = RecorderResolver(self.env, self.writer, self.redis_client, self.metric)
        self.live_state_service = LiveStateService(self.redis_client)

    async def run(self):
        log.set_level(logging.DEBUG)
        disable_streamlink_log()

        if self.env.config_path is None:
            raise ValueError("Config path not set")
        conf = read_conf(self.env.config_path)

        state = await self.get_state(conf)
        await self.live_state_service.set(state, nx=False)
        recorder = self.recorder_resolver.create_recorder(state=state)
        recorder.record()

        if self.env.env == "dev":
            await async_input("Press any key to exit")
            recorder.state.cancel()
            if recorder.recording_task is not None:
                await recorder.recording_task

    async def get_state(self, conf: BatchConfig):
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

        if len(self.fetcher.headers) == 0:
            self.fetcher.set_headers(headers)

        live = await self.fetcher.fetch_live_info(conf.url)
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
