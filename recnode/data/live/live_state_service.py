from redis.asyncio import Redis

from .live_state import LiveState
from ..redis import RedisString, inc_count

KEY_PREFIX = "live"


class LiveStateService:
    def __init__(self, master: Redis, replica: Redis):
        self.__master = master
        self.__replica = replica
        self.__str_master = RedisString(self.__master)
        self.__str_replica = RedisString(self.__replica)

    async def get_live(self, record_id: str, use_master: bool) -> LiveState | None:
        inc_count(use_master=use_master)
        text = await self.__get_str_redis(use_master).get(self.__get_key(record_id))
        if text is None:
            return None
        return LiveState.parse_raw(text)

    async def update_is_invalid(self, record_id: str, is_invalid: bool) -> bool:
        live = await self.get_live(record_id=record_id, use_master=True)
        if live is not None:
            inc_count(use_master=True)
            live.is_invalid = is_invalid
            await self.set_live(live, nx=False)
            return True
        return False

    async def set_live(self, state: LiveState, nx: bool, px: int | None = None) -> bool:
        key = self.__get_key(state.id)
        req_px = px
        inc_count(use_master=False)
        ttl = await self.__replica.pttl(key)
        if ttl > 0:
            req_px = ttl
        inc_count(use_master=True)
        text = state.model_dump_json(by_alias=True)
        return await self.__str_master.set(key, text, nx=nx, px=req_px)

    async def delete(self, record_id: str, check_replica: bool = False):
        key = self.__get_key(record_id)
        if check_replica:
            inc_count(use_master=False)
            if not await self.__str_replica.exists(key):
                return
        inc_count(use_master=True)
        await self.__str_master.delete(key)  # If replica check is performed, data might not be deleted

    def __get_key(self, record_id: str) -> str:
        return f"{KEY_PREFIX}:{record_id}"

    def __get_str_redis(self, use_master: bool = True) -> RedisString:
        return self.__str_master if use_master else self.__str_replica
