import pytest

from stdl.fetcher import TwitchFetcher

channel_id = "tototmix"


@pytest.mark.asyncio
async def test_twitch_fetcher():
    print()
    fetcher = TwitchFetcher()
    # data = await fetcher.metadata_channel_raw(channel_id)
    # print(json.dumps(data, indent=2))
    info = await fetcher.metadata_channel(channel_id)
    print(info)
