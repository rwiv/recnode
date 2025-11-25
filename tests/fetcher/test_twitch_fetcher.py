import pytest

from recnode.fetcher import TwitchFetcher
from recnode.utils import AsyncHttpClient

channel_id = ""


@pytest.mark.asyncio
async def test_twitch_fetcher():
    print()
    fetcher = TwitchFetcher(AsyncHttpClient())
    # data = await fetcher.metadata_channel_raw(channel_id)
    # print(json.dumps(data, indent=2))
    info = await fetcher.metadata_channel(channel_id, headers={})
    if info is not None:
        print(info.live_id)
