import pytest

from stdl.fetcher.platform.soop_fetcher import SoopFetcher
from stdl.utils import FIREFOX_USER_AGENT

live_url = ""


@pytest.mark.asyncio
async def test_platform_fetcher():
    print()
    fetcher = SoopFetcher()
    headers = {"User-Agent": FIREFOX_USER_AGENT}
    info = await fetcher.fetch_live_info(live_url, headers=headers)
    print(info)
