import asyncio
import logging

from pyutils import log
from redis.asyncio import Redis

from .stream_utils import get_state, read_conf
from ..config import get_env
from ..data.live import LiveStateService
from ..data.redis import create_redis_pool
from ..file import create_fs_writer
from ..recorder import RecorderResolver
from ..utils import disable_streamlink_log


async def async_input(prompt: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)


class BatchRunner:
    def __init__(self):
        self.env = get_env()
        self.writer = create_fs_writer(self.env)
        self.recorder_resolver = RecorderResolver(self.env, self.writer)

    async def run(self):
        log.set_level(logging.DEBUG)
        disable_streamlink_log()

        if self.env.config_path is None:
            raise ValueError("Config path not set")
        conf = read_conf(self.env.config_path)

        live_state_service = LiveStateService(
            master=Redis(connection_pool=create_redis_pool(self.env.redis_master)),
            replica=Redis(connection_pool=create_redis_pool(self.env.redis_replica)),
        )

        state = await get_state(url=conf.url, cookie_header=conf.cookie)
        await live_state_service.set_live(state, nx=False, px=int(self.env.redis_data.live_expire_sec * 1000))

        recorder = self.recorder_resolver.create_recorder(state=state)
        recorder.record()

        if self.env.env == "dev":
            await async_input("Press any key to exit")
            recorder.state.cancel()
            if recorder.recording_thread is not None:
                recorder.recording_thread.join()

        while True:
            if recorder.is_done:
                if recorder.recording_thread is not None:
                    recorder.recording_thread.join()
                log.info("Recording done")
                break
            await asyncio.sleep(1)
