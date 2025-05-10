import json

from pydantic import BaseModel
from redis.asyncio import Redis

from ..redis import RedisString, RedisPubSubLock


class SegmentState(BaseModel):
    url: str
    num: int
    duration: float
    size: int
    # parallel_limit: int
    # retry_count: int


class SegmentStateService:
    def __init__(
        self,
        client: Redis,
        live_record_id: str,
        expire_ms: int,
        lock_expire_ms: int = 2_000,
        lock_wait_timeout_sec: int = 5,
    ):
        self.__client = client
        self.__str = RedisString(client)
        self.__live_record_id = live_record_id
        self.__expire_ms = expire_ms
        self.__lock_expire_ms = lock_expire_ms
        self.__lock_wait_timeout_sec = lock_wait_timeout_sec

    async def renew(self, num: int):
        await self.__str.set_pexpire(self.__get_key(num), self.__expire_ms)

    async def get(self, num: int) -> SegmentState | None:
        txt = await self.__str.get(self.__get_key(num))
        if txt is None:
            return None
        return SegmentState(**json.loads(txt))

    async def set_nx(self, state: SegmentState) -> bool:
        return await self.__str.set(
            key=self.__get_key(state.num),
            value=state.model_dump_json(by_alias=True),
            nx=True,
            ex=self.__expire_ms,
        )

    def new_lock(self, num: int) -> RedisPubSubLock:
        return RedisPubSubLock(
            client=self.__client,
            key=f"{self.__get_key(num)}:lock",
            expire_ms=self.__lock_expire_ms,
            timeout_sec=self.__lock_wait_timeout_sec,
        )

    async def update(self, state: SegmentState) -> bool:
        async with self.new_lock(state.num):
            return await self.__str.set(
                key=self.__get_key(state.num),
                value=state.model_dump_json(by_alias=True),
                ex=self.__expire_ms,
            )

    async def delete(self, num: int) -> bool:
        async with self.new_lock(num):
            return await self.__str.delete(self.__get_key(num))

    def __get_key(self, num: int) -> str:
        return f"live:{self.__live_record_id}:segment:{num}"
