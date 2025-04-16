import pytest
from streamlink.session.session import Streamlink

from stdl.fetcher import SoopFetcher


channel_id = ""


@pytest.mark.asyncio
async def test_soop_fetcher():
    print()
    fetcher = SoopFetcher()
    info = await fetcher.fetch_live_info(channel_id, Streamlink().http.headers)
    print(info)
