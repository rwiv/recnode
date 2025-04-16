from stdl.fetcher import TwitchFetcher


channel_id = ""


def test_twitch_fetcher():
    print()
    fetcher = TwitchFetcher()
    info = fetcher.metadata_channel(channel_id)
    print(info)
