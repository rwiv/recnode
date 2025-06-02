import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.live import LiveStateService
from stdl.data.redis import create_redis_pool
from tests.data.mock_helpers import live

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()
master = Redis(connection_pool=create_redis_pool(env.redis_master))
replica = Redis(connection_pool=create_redis_pool(env.redis_replica))


@pytest.mark.asyncio
async def test_live_state_service():
    live_service = LiveStateService(master=master, replica=replica)
    live1 = live(id="2f208071-d46f-4632-b962-d69034321b23")
    await live_service.delete(live1.id)

    assert await live_service.set(live1, nx=True)
    assert await live_service.pttl(live1.id) == -1

    assert await live_service.set(live1, nx=False, px=10_000)
    assert await live_service.pttl(live1.id) > 100

    assert await live_service.set(live1, nx=False)
    assert await live_service.pttl(live1.id) != -1

    await live_service.delete(live1.id)
    assert await live_service.get(live1.id) is None
