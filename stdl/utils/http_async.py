from typing import Any

import aiohttp


async def fetch_bytes(url: str, headers: dict) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as res:
            if res.status >= 400:
                raise ValueError(f"Failed to fetch stream: {res.status} {res.reason}")
            return await res.read()


async def fetch_text(url: str, headers: dict) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as res:
            if res.status >= 400:
                raise ValueError(f"Failed to fetch stream: {res.status} {res.reason}")
            return await res.text()


async def fetch_json(url: str, headers: dict) -> Any:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as res:
            if res.status >= 400:
                raise ValueError(f"Failed to fetch stream: {res.status} {res.reason}")
            return await res.json()
