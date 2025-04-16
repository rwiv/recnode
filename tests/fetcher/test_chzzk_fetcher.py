import pytest
from streamlink.session.session import Streamlink

from stdl.fetcher import ChzzkFetcher


channel_id = ""


@pytest.mark.asyncio
async def test_chzzk_fetcher():
    fetcher = ChzzkFetcher()
    info = await fetcher.fetch_live_info(channel_id, Streamlink().http.headers)
    print(info)
