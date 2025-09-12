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
from tests.data.mock_helpers import seg, live

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()
master = Redis(connection_pool=create_redis_pool(env.redis_master))
replica = Redis(connection_pool=create_redis_pool(env.redis_replica))

ex = 10_000
lw = 2


@pytest.mark.asyncio
async def test_validate_segments():
    log.set_level(logging.DEBUG)
    live_record_id = "cc23b367-bc45-40cd-9523-e334b1bcd52d"
    success_nums = SegmentNumberSet(master, replica, live_record_id, "success", ex, ex, lw, {})
    http_mock = AsyncHttpClientMock(b_size=100)
    invalid_seg_num_diff_threshold = 150
    live_service = LiveStateService(master=master, replica=replica)
    seg_service = SegmentStateService(master, replica, live_record_id, ex, ex, 3, {})
    validator = SegmentStateValidator(live_service, seg_service, http_mock, {}, 120, invalid_seg_num_diff_threshold)

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)

    await live_service.set_live(live(id=live_record_id), nx=True)

    now = datetime.now()
    await seg_service.set_seg_nx(seg(301))
    await success_nums.set_num(301)
    await seg_service.set_seg_nx(seg(302))
    await success_nums.set_num(302)
    await seg_service.set_seg_nx(seg(303, size=200))
    await success_nums.set_num(303)
    await seg_service.set_seg_nx(seg(304, size=None))
    await success_nums.set_num(304)
    await seg_service.set_seg_nx(seg(305, created_at=now - timedelta(seconds=200)))
    await success_nums.set_num(305)
    await seg_service.set_seg_nx(seg(306))
    await success_nums.set_num(306)
    await seg_service.set_seg_nx(seg(307, size=None))
    await success_nums.set_num(307)

    l = await success_nums.get_highest(use_master=True)

    # check for empty segments
    assert await validator.validate_segments([seg(302), seg(304)], l, success_nums) == ok()

    # check for segment url
    assert await validator.validate_segments([seg(302), seg(304, url="asd")], l, success_nums) == crit()
    # check for segment duration
    assert await validator.validate_segments([seg(302), seg(304, duration=5.3)], l, success_nums) == crit()

    # check for segment size
    assert await validator.validate_segments([seg(301), seg(302), seg(303), seg(304)], l, success_nums) == crit()
    assert await validator.validate_segments([seg(302), seg(303), seg(306), seg(307)], l, success_nums) == ok()

    # check for segment timestamp
    assert await validator.validate_segments([seg(302), seg(305)], l, success_nums) == crit()

    # check for seg_num_diff_threshold
    assert await validator.validate_segments([seg(400), seg(401)], l, success_nums) == ok()
    assert await validator.validate_segments([seg(460), seg(462), seg(464)], l, success_nums) == crit()
    assert await validator.validate_segments([seg(100), seg(102)], l, success_nums) == crit()

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)


@pytest.mark.asyncio
async def test_validate_segment():
    log.set_level(logging.DEBUG)
    live_record_id = "31cad56f-3d77-41d6-85b1-0cc77272aac0"
    success_nums = SegmentNumberSet(master, replica, live_record_id, "success", ex, ex, lw, {})
    http_mock = AsyncHttpClientMock(b_size=100)
    invalid_seg_time_diff_threshold_sec = 2 * 60
    live_service = LiveStateService(master=master, replica=replica)
    seg_service = SegmentStateService(master, replica, live_record_id, ex, ex, 3, {})
    validator = SegmentStateValidator(live_service, seg_service, http_mock, {}, invalid_seg_time_diff_threshold_sec)

    await seg_service.delete_mapped(success_nums)

    now = datetime.now()
    await seg_service.set_seg_nx(seg(301))
    await success_nums.set_num(301)
    await seg_service.set_seg_nx(seg(302, created_at=now - timedelta(seconds=50)))
    await success_nums.set_num(302)
    await seg_service.set_seg_nx(seg(303, created_at=now - timedelta(seconds=200)))
    await success_nums.set_num(303)

    l = await success_nums.get_highest(use_master=True)

    assert await validator.validate_segment(seg(304), l, success_nums) == ok()
    assert await validator.validate_segment(seg(302), l, success_nums) == no()
    assert await validator.validate_segment(seg(303), l, success_nums) == crit()
    assert await validator.validate_segment(seg(102), l, success_nums) == crit()

    await seg_service.delete_mapped(success_nums)
