import pytest
from pyutils import load_dotenv, path_join, find_project_root

from recnode.file import ProxyObjectWriter

load_dotenv(path_join(find_project_root(), "dev", ".env"))


@pytest.mark.asyncio
async def test_proxy_writer():
    proxy_endpoint = "http://localhost:9033"
    fs_name = "test-fs"
    writer = ProxyObjectWriter(endpoint=proxy_endpoint, fs_name=fs_name)
    path = "incomplete/uid1/vid1/test.txt"
    await writer.write(path=path, data=b"test")
