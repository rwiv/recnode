import pytest
from aiohttp_socks import ProxyType
from pyutils import load_dotenv, path_join, find_project_root

from stdl.config import get_env
from stdl.utils import AsyncHttpClient, ProxyConnectorConfig

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()


@pytest.mark.asyncio
async def test_http_client():
    print()
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
    http_client = AsyncHttpClient(proxy=proxy)
    res = await http_client.get_text("https://api.ipify.org")
    print(res)
