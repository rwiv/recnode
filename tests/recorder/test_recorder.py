import aiohttp
import pytest
from pyutils import load_dotenv, path_join, find_project_root

load_dotenv(path_join(find_project_root(), "dev", ".env"))

uid = ""
worker_url = "http://localhost:9083/api/recordings"


@pytest.mark.asyncio
async def test_post_record():
    print()
    record_id = ""
    async with aiohttp.ClientSession() as session:
        async with session.post(url=f"{worker_url}:{record_id}") as res:
            print(await res.text())


@pytest.mark.asyncio
async def test_delete_record():
    print()
    record_id = ""
    async with aiohttp.ClientSession() as session:
        async with session.delete(url=f"{worker_url}:{record_id}") as res:
            print(await res.text())
