import pytest
from aiohttp_socks import ProxyConnector, ProxyType
from pyutils import load_dotenv, path_join, find_project_root

from stdl.config import get_env
from stdl.utils import AsyncHttpClient

load_dotenv(path_join(find_project_root(), "dev", ".env"))
env = get_env()


@pytest.mark.asyncio
async def test_http_client():
    print()
    conf = env.proxy
    host = conf.host
    if host is None:
        raise ValueError("Proxy host is not set")

    connector = ProxyConnector(
        proxy_type=ProxyType.SOCKS5,
        host=host,
        # port=conf.port_domestic,
        port=conf.port_overseas,
        username=conf.username,
        password=conf.password,
        rdns=conf.rdns,
    )
    http_client = AsyncHttpClient(connector=connector)
    res = await http_client.get_text("https://api.ipify.org")
    print(res)
