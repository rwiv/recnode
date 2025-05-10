from datetime import datetime, timedelta

import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.redis import (
    RedisString,
    RedisQueue,
    create_redis_pool,
    RedisSortedSet,
)
from stdl.data.segment import SegmentNumberSet, SegmentStateService, SegmentState

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)

redis_str = RedisString(client)
redis_queue = RedisQueue(client)
redis_sorted_set = RedisSortedSet(client)

ex = 10_000
lw = 2


@pytest.mark.asyncio
async def test_validate_segment():
    live_record_id = "31cad56f-3d77-41d6-85b1-0cc77272aac0"
    success_nums = SegmentNumberSet(client, live_record_id, "success", ex, ex, lw)
    seg_service = SegmentStateService(client, live_record_id, ex, ex, lw)

    await seg_service.set_nx(seg(1))
    await success_nums.set(1)
    await seg_service.set_nx(seg(2, created_at=datetime.now() - timedelta(seconds=200)))
    await success_nums.set(2)
    await seg_service.set_nx(seg(3, created_at=datetime.now() - timedelta(seconds=50)))
    await success_nums.set(3)

    assert await seg_service.validate_segment(4, success_nums) == (True, False)
    assert await seg_service.validate_segment(3, success_nums) == (False, False)
    assert await seg_service.validate_segment(2, success_nums) == (False, True)

    await seg_service.delete_mapped(success_nums)


@pytest.mark.asyncio
async def test_validate_segments():
    live_record_id = "cc23b367-bc45-40cd-9523-e334b1bcd52d"
    success_nums = SegmentNumberSet(client, live_record_id, "success", ex, ex, lw)
    seg_service = SegmentStateService(client, live_record_id, ex, ex, lw)


def seg(num: int, created_at: datetime = datetime.now()):
    return SegmentState(
        url="https://example.com",
        num=num,
        duration=2.0,
        size=100,
        created_at=created_at,
        updated_at=datetime.now(),
    )
