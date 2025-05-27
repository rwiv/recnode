import uuid
from datetime import datetime

from stdl.common import PlatformType
from stdl.data.live import LiveState
from stdl.data.segment import SegmentState


def live(id: str = str(uuid.uuid4())):
    return LiveState(
        id=id,
        platform=PlatformType.CHZZK,
        channelId="123456",
        channelName="test_channel",
        liveId="123456789",
        liveTitle="test_title",
        streamUrl="http://example.com/stream",
        headers=None,
        videoName="test_video",
        isInvalid=False,
        createdAt=datetime.now(),
        updatedAt=datetime.now(),
    )


def seg(
    num: int,
    created_at: datetime = datetime.now(),
    url: str = "https://example.com",
    duration: float = 2.0,
    size: int = 100,
):
    return SegmentState(
        url=url,
        num=num,
        duration=duration,
        size=size,
        parallel_limit=1,
        created_at=created_at,
        updated_at=datetime.now(),
    )
