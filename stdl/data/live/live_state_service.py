from redis.asyncio import Redis

from .live_state import LiveState
from ..redis import RedisString


KEY_PREFIX = "live"


class LiveStateService:
    def __init__(self, client: Redis):
        self.__client = RedisString(client)

    async def get(self, record_id: str) -> LiveState | None:
        text = await self.__client.get(f"{KEY_PREFIX}:{record_id}")
        if text is None:
            return None
        return LiveState.parse_raw(text)

    async def set(self, state: LiveState):
        text = state.model_dump_json(by_alias=True)
        await self.__client.set(f"{KEY_PREFIX}:{state.id}", text, nx=True)

    async def delete(self, record_id: str):
        await self.__client.delete(f"{KEY_PREFIX}:{record_id}")
