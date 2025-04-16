import pytest

from stdl.fetcher import PlatformFetcher


live_url = ""


@pytest.mark.asyncio
async def test_platform_fetcher():
    print()
    fetcher = PlatformFetcher()
    info = await fetcher.fetch_live_info(live_url)
    print(info)
