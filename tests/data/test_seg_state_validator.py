import logging
from datetime import datetime, timedelta

import pytest
from pyutils import load_dotenv, path_join, find_project_root, log
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.live import LiveStateService
from stdl.data.redis import create_redis_pool
from stdl.data.segment import SegmentNumberSet, SegmentStateService, SegmentStateValidator, ok, no, critical as crit
from stdl.utils import AsyncHttpClientMock
from tests.data.mock_helpers import s1, s2, live

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()
conf = env.redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)

ex = 10_000
lw = 2


@pytest.mark.asyncio
async def test_validate_segments():
    log.set_level(logging.DEBUG)
    live_record_id = "cc23b367-bc45-40cd-9523-e334b1bcd52d"
    success_nums = SegmentNumberSet(client, live_record_id, "success", ex, ex, lw, {})
    http_mock = AsyncHttpClientMock(b_size=100)
    invalid_seg_num_diff_threshold = 150
    live_service = LiveStateService(client)
    seg_service = SegmentStateService(client, live_record_id, ex, ex, lw, {})
    validator = SegmentStateValidator(live_service, seg_service, http_mock, {}, 120, invalid_seg_num_diff_threshold)

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)

    await live_service.set(live(id=live_record_id), nx=True)

    now = datetime.now()
    await seg_service.set_nx(s2(301))
    await success_nums.set(301)
    await seg_service.set_nx(s2(302))
    await success_nums.set(302)
    await seg_service.set_nx(s2(303, size=200))
    await success_nums.set(303)
    await seg_service.set_nx(s2(304))
    await success_nums.set(304)
    await seg_service.set_nx(s2(305, created_at=now - timedelta(seconds=200)))
    await success_nums.set(305)

    l = await success_nums.get_highest()

    assert await validator.validate_segments([s1(302), s1(304)], l, success_nums) == ok()
    assert await validator.validate_segments([s1(302), s1(304, url="asd")], l, success_nums) == crit()

    assert await validator.validate_segments([s1(302), s1(304, duration=5.3)], l, success_nums) == crit()

    assert await validator.validate_segments([s1(302), s1(303)], l, success_nums) == crit()

    # Pass if there are segments with different sizes but they are not the last number
    assert await validator.validate_segments([s1(302), s1(303), s1(304)], l, success_nums) == ok()

    assert await validator.validate_segments([s1(302), s1(305)], l, success_nums) == crit()

    assert await validator.validate_segments([s1(400), s1(401)], l, success_nums) == ok()
    assert await validator.validate_segments([s1(460), s1(462), s1(464)], l, success_nums) == crit()

    assert await validator.validate_segments([s1(100), s1(102)], l, success_nums) == crit()

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)


@pytest.mark.asyncio
async def test_validate_segment():
    log.set_level(logging.DEBUG)
    live_record_id = "31cad56f-3d77-41d6-85b1-0cc77272aac0"
    success_nums = SegmentNumberSet(client, live_record_id, "success", ex, ex, lw, {})
    http_mock = AsyncHttpClientMock(b_size=100)
    invalid_seg_time_diff_threshold_sec = 2 * 60
    live_service = LiveStateService(client)
    seg_service = SegmentStateService(client, live_record_id, ex, ex, lw, {})
    validator = SegmentStateValidator(live_service, seg_service, http_mock, {}, invalid_seg_time_diff_threshold_sec)

    await seg_service.delete_mapped(success_nums)

    now = datetime.now()
    await seg_service.set_nx(s2(301))
    await success_nums.set(301)
    await seg_service.set_nx(s2(302, created_at=now - timedelta(seconds=50)))
    await success_nums.set(302)
    await seg_service.set_nx(s2(303, created_at=now - timedelta(seconds=200)))
    await success_nums.set(303)

    l = await success_nums.get_highest()

    assert await validator.validate_segment(s1(304), l, success_nums) == ok()
    assert await validator.validate_segment(s1(302), l, success_nums) == no()
    assert await validator.validate_segment(s1(303), l, success_nums) == crit()
    assert await validator.validate_segment(s1(102), l, success_nums) == crit()

    await seg_service.delete_mapped(success_nums)
