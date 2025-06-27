import pytest

from stdl.fetcher import SoopFetcher
from stdl.utils import FIREFOX_USER_AGENT, AsyncHttpClient

channel_id = ""


@pytest.mark.asyncio
async def test_soop_fetcher():
    print()
    fetcher = SoopFetcher(AsyncHttpClient())
    headers = {"User-Agent": FIREFOX_USER_AGENT}
    info = await fetcher.fetch_live_info(channel_id, headers=headers)
    print(info)
