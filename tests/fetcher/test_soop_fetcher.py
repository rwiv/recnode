import pytest
from streamlink.session.session import Streamlink

from stdl.fetcher import SoopFetcher


channel_id = ""


@pytest.mark.asyncio
async def test_soop_fetcher():
    print()
    fetcher = SoopFetcher()
    headers = {}
    for k, v in Streamlink().http.headers:
        headers[k] = v
    info = await fetcher.fetch_live_info(channel_id, headers=headers)
    print(info)
