from redis.asyncio import Redis

from .live_state import LiveState
from ..redis import RedisString


KEY_PREFIX = "live"


class LiveStateService:
    def __init__(self, client: Redis):
        self.__client = client
        self.__str = RedisString(client)

    async def pttl(self, record_id: str) -> int:
        return await self.__client.pttl(self.__get_key(record_id))

    async def get(self, record_id: str) -> LiveState | None:
        text = await self.__str.get(self.__get_key(record_id))
        if text is None:
            return None
        return LiveState.parse_raw(text)

    async def update_is_invalid(self, record_id: str, is_invalid: bool) -> bool:
        live = await self.get(record_id=record_id)
        if live is not None:
            live.is_invalid = is_invalid
            await self.set(live, nx=False)
            return True
        return False

    async def set(self, state: LiveState, nx: bool, px: int | None = None) -> bool:
        key = self.__get_key(state.id)
        req_px = px
        ttl = await self.__client.pttl(key)
        if ttl > 0:
            req_px = ttl
        text = state.model_dump_json(by_alias=True, exclude_none=True)
        return await self.__str.set(key, text, nx=nx, px=req_px)

    async def delete(self, record_id: str) -> int:
        return await self.__str.delete(self.__get_key(record_id))

    def __get_key(self, record_id: str) -> str:
        return f"{KEY_PREFIX}:{record_id}"
