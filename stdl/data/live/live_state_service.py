from redis.asyncio import Redis

from .live_state import LiveState
from ..redis import RedisString


KEY_PREFIX = "live"


class LiveStateService:
    def __init__(self, master: Redis, replica: Redis):
        self.__master = master
        self.__replica = replica
        self.__str_master = RedisString(self.__master)
        self.__str_replica = RedisString(self.__replica)

    async def get_live(self, record_id: str, use_master: bool) -> LiveState | None:
        str_redis = self.__str_master if use_master else self.__str_replica
        text = await str_redis.get(self.__get_key(record_id))
        if text is None:
            return None
        return LiveState.parse_raw(text)

    async def update_is_invalid(self, record_id: str, is_invalid: bool) -> bool:
        live = await self.get_live(record_id=record_id, use_master=True)
        if live is not None:
            live.is_invalid = is_invalid
            await self.set_live(live, nx=False)
            return True
        return False

    async def set_live(self, state: LiveState, nx: bool, px: int | None = None) -> bool:
        key = self.__get_key(state.id)
        req_px = px
        ttl = await self.__replica.pttl(key)
        if ttl > 0:
            req_px = ttl
        text = state.model_dump_json(by_alias=True, exclude_none=True)
        return await self.__str_master.set(key, text, nx=nx, px=req_px)

    async def delete(self, record_id: str):
        key = self.__get_key(record_id)
        if await self.__str_replica.contains(key):
            await self.__str_master.delete(key)

    def __get_key(self, record_id: str) -> str:
        return f"{KEY_PREFIX}:{record_id}"
