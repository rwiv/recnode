import asyncio
import logging

from pyutils import log
from redis.asyncio import Redis

from .stream_utils import get_live_state, read_conf
from ..config import get_env
from ..data.live import LiveStateService
from ..data.redis import create_redis_pool
from ..file import create_fs_writer
from ..recorder import RecorderResolver
from ..utils import disable_streamlink_log, fetch_my_public_ip


async def async_input(prompt: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)


class BatchRunner:
    def __init__(self):
        self.__env = get_env()
        self.__writer = create_fs_writer(self.__env)
        my_public_ip = fetch_my_public_ip()
        self.__recorder_resolver = RecorderResolver(self.__env, self.__writer, my_public_ip)

    async def run(self):
        log.set_level(logging.DEBUG)
        disable_streamlink_log()

        if self.__env.config_path is None:
            raise ValueError("Config path not set")
        conf = read_conf(self.__env.config_path)

        live_service = LiveStateService(
            master=Redis(connection_pool=create_redis_pool(self.__env.redis_master)),
            replica=Redis(connection_pool=create_redis_pool(self.__env.redis_replica)),
        )

        live_state = await get_live_state(live_url=conf.url, stream_params_str=conf.params, platform_cookie=conf.cookie)
        await live_service.set_live(live_state, nx=False, px=int(self.__env.redis_data.live_expire_sec * 1000))

        recorder = self.__recorder_resolver.create_recorder(state=live_state)
        recorder.record()

        if self.__env.env == "dev":
            await async_input("Press any key to exit")
            recorder.cancel()
            if recorder.recording_thread is not None:
                recorder.recording_thread.join()

        while True:
            if recorder.is_done:
                if recorder.recording_thread is not None:
                    recorder.recording_thread.join()
                log.info("Recording done")
                break
            await asyncio.sleep(1)
