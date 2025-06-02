import asyncio

import aiohttp
import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.app import get_state, read_conf
from stdl.config import get_env
from stdl.data.live import LiveStateService
from stdl.data.redis import create_redis_pool

load_dotenv(path_join(find_project_root(), "dev", ".env"))

env = get_env()
live_state_service = LiveStateService(Redis(connection_pool=create_redis_pool(env.redis_master)))

if env.config_path is None:
    raise ValueError("Config path not set")
conf = read_conf(env.config_path)

worker_url1 = "http://localhost:13201/api/recordings"
worker_url2 = "http://localhost:13202/api/recordings"
worker_url3 = "http://localhost:13203/api/recordings"

record_id = ""


@pytest.mark.asyncio
async def test_post_record():
    print()
    state = await get_state(url=conf.url, cookie_header=conf.cookie)
    await live_state_service.set(state, nx=False, px=int(env.redis_data.live_expire_sec * 1000))
    print(state.id)

    await start(f"{worker_url1}/{state.id}")
    await asyncio.sleep(5)
    await start(f"{worker_url2}/{state.id}")
    await start(f"{worker_url3}/{state.id}")


@pytest.mark.asyncio
async def test_delete_record():
    print()
    await cancel(f"{worker_url1}/{record_id}")
    await cancel(f"{worker_url2}/{record_id}")
    await cancel(f"{worker_url3}/{record_id}")


async def start(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(url=url) as res:
            print(await res.text())


async def cancel(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.delete(url=url) as res:
            print(await res.text())
