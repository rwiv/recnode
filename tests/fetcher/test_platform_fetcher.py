from stdl.fetcher import PlatformFetcher


live_url = ""


def test_platform_fetcher():
    print()
    fetcher = PlatformFetcher()
    info = fetcher.fetch_live_info(live_url)
    print(info)
