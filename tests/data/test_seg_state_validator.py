from datetime import datetime, timedelta

import pytest
from pyutils import load_dotenv, path_join, find_project_root
from redis.asyncio import Redis

from stdl.config import get_env
from stdl.data.live import LiveStateService
from stdl.data.redis import create_redis_pool
from stdl.data.segment import SegmentNumberSet, SegmentStateService, SegmentStateValidator, SegmentInspect
from stdl.utils import AsyncHttpClientMock
from tests.data.mock_helpers import seg, seg2, live

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()
conf = env.redis
pool = create_redis_pool(conf)
client = Redis(connection_pool=pool)

ex = 10_000
lw = 2


@pytest.mark.asyncio
async def test_validate_segments():
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

    src_live = await live_service.get(live_record_id)
    if src_live is None:
        raise Exception("LiveState not found")
    assert src_live.is_invalid is None

    await seg_service.set_nx(seg(1))
    await success_nums.set(1)
    await seg_service.set_nx(seg(2))
    await success_nums.set(2)
    await seg_service.set_nx(seg(3, size=200))
    await success_nums.set(3)
    await seg_service.set_nx(seg(4))
    await success_nums.set(4)
    await seg_service.set_nx(seg(5))
    await success_nums.set(5)

    assert await validator.validate_segments([seg2(2), seg2(4)], success_nums)
    assert not await validator.validate_segments([seg2(2), seg2(4, url="asd")], success_nums)
    await live_service.update_to_invalid_live(live_record_id, False)
    assert not await validator.validate_segments([seg2(2), seg2(4, duration=5.3)], success_nums)
    await live_service.update_to_invalid_live(live_record_id, False)
    assert not await validator.validate_segments([seg2(2), seg2(3)], success_nums)
    await live_service.update_to_invalid_live(live_record_id, False)
    # Pass if there are segments with different sizes but they are not the last number
    assert await validator.validate_segments([seg2(2), seg2(3), seg2(4)], success_nums)
    await live_service.update_to_invalid_live(live_record_id, False)

    assert await validator.validate_segments([seg2(100), seg2(101)], success_nums)
    assert not await validator.validate_segments([seg2(160), seg2(162), seg2(164)], success_nums)

    src_live = await live_service.get(live_record_id)
    if src_live is None:
        raise Exception("LiveState not found")
    assert src_live.is_invalid

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)


@pytest.mark.asyncio
async def test_validate_segment():
    live_record_id = "31cad56f-3d77-41d6-85b1-0cc77272aac0"
    success_nums = SegmentNumberSet(client, live_record_id, "success", ex, ex, lw, {})
    http_mock = AsyncHttpClientMock(b_size=100)
    invalid_seg_time_diff_threshold_sec = 2 * 60
    live_service = LiveStateService(client)
    seg_service = SegmentStateService(client, live_record_id, ex, ex, lw, {})
    validator = SegmentStateValidator(live_service, seg_service, http_mock, {}, invalid_seg_time_diff_threshold_sec)

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)

    await live_service.set(live(id=live_record_id), nx=True)

    src_live = await live_service.get(live_record_id)
    if src_live is None:
        raise Exception("LiveState not found")
    assert src_live.is_invalid is None

    now = datetime.now()
    await seg_service.set_nx(seg(1))
    await success_nums.set(1)
    await seg_service.set_nx(seg(2, created_at=now - timedelta(seconds=200)))
    await success_nums.set(2)
    await seg_service.set_nx(seg(3, created_at=now - timedelta(seconds=50)))
    await success_nums.set(3)

    assert await validator.validate_segment(4, success_nums) == SegmentInspect(ok=True, critical=False)
    assert await validator.validate_segment(3, success_nums) == SegmentInspect(ok=False, critical=False)
    assert await validator.validate_segment(2, success_nums) == SegmentInspect(ok=False, critical=True)

    src_live = await live_service.get(live_record_id)
    if src_live is None:
        raise Exception("LiveState not found")
    assert src_live.is_invalid

    await seg_service.delete_mapped(success_nums)
    await live_service.delete(live_record_id)
