from aiohttp_socks import ProxyType
import pytest
from pyutils import load_dotenv, path_join, find_project_root

from stdl.config import get_env
from stdl.fetcher import ChzzkFetcher
from stdl.utils import FIREFOX_USER_AGENT, AsyncHttpClient, ProxyConnectorConfig

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()
channel_id = ""


@pytest.mark.asyncio
async def test_chzzk_fetcher():
    conf = env.proxy
    host = conf.host
    if host is None:
        raise ValueError("Proxy host is not set")

    proxy = ProxyConnectorConfig(
        proxy_type=ProxyType.SOCKS5,
        host=host,
        port=conf.port_domestic,
        username=conf.username,
        password=conf.password,
        rdns=conf.rdns,
    )

    fetcher = ChzzkFetcher(AsyncHttpClient(proxy=proxy))
    headers = {"User-Agent": FIREFOX_USER_AGENT}
    info = await fetcher.fetch_live_info(channel_id, headers=headers)
    print(info)
