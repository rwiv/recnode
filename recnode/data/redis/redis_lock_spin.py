import asyncio
import uuid

import redis.asyncio as redis


class RedisSpinLock:
    _RELEASE_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
    """

    def __init__(
        self,
        client: redis.Redis,
        key: str,
        expire_ms: int,
        timeout_sec: float = 10,
        retry_sec: float = 0.1,
    ):
        self.__client = client
        self.__key = key
        self.__expire_ms = expire_ms
        self.__timeout_sec = timeout_sec
        self.__retry_sec = retry_sec
        self.__token: str | None = None

    async def acquire(self) -> bool:
        self.__token = str(uuid.uuid4())
        end = asyncio.get_event_loop().time() + self.__timeout_sec
        while asyncio.get_event_loop().time() < end:
            ok = await self.__client.set(self.__key, self.__token, nx=True, px=self.__expire_ms)
            if ok:
                return True
            await asyncio.sleep(self.__retry_sec)
        return False

    async def extend(self, extra_time: int) -> bool:
        if not self.__token:
            raise ValueError("Lock not acquired")

        ttl = await self.__client.pttl(self.__key)
        if ttl > 0:
            val = await self.__client.get(self.__key)
            if isinstance(val, bytes):
                val = val.decode()
            if val and val == self.__token:
                return await self.__client.set(self.__key, self.__token, xx=True, px=ttl + extra_time)
        return False

    async def release(self) -> bool:
        if not self.__token:
            return False
        result = await self.__client.eval(self._RELEASE_SCRIPT, 1, self.__key, self.__token)  # type: ignore
        return result == 1

    async def __aenter__(self) -> "RedisSpinLock":
        acquired = await self.acquire()
        if not acquired:
            raise TimeoutError(f"Failed to acquire lock '{self.__key}'")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()
