from streamlink.session.session import Streamlink

from stdl.fetcher import SoopFetcher


channel_id = ""


def test_soop_fetcher():
    print()
    fetcher = SoopFetcher()
    info = fetcher.fetch_live_info(channel_id, Streamlink().http.headers)
    print(info)
