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
        expire_sec: int,
        wait_timeout_ms: int = 10_000,  # 획득 시도 최대 대기시간(밀리초)
        retry_interval: float = 0.1,  # 재시도 간격(초)
    ):
        self._client = client
        self._key = key
        self._expire_ms = expire_sec
        self._wait_timeout_ms = wait_timeout_ms
        self._retry_interval_sec = retry_interval
        self._token: str | None = None

    async def acquire(self) -> bool:
        self._token = str(uuid.uuid4())
        end = asyncio.get_event_loop().time() + (self._wait_timeout_ms / 1000)
        while asyncio.get_event_loop().time() < end:
            ok = await self._client.set(self._key, self._token, nx=True, px=self._expire_ms)
            if ok:
                return True
            await asyncio.sleep(self._retry_interval_sec)
        return False

    async def extend(self, extra_time: int) -> bool:
        if not self._token:
            raise ValueError("Lock not acquired")

        ttl = await self._client.pttl(self._key)
        if ttl > 0:
            val = await self._client.get(self._key)
            if isinstance(val, bytes):
                val = val.decode()
            if val and val == self._token:
                return await self._client.set(self._key, self._token, xx=True, px=ttl + extra_time)
        return False

    async def release(self) -> bool:
        if not self._token:
            return False
        result = await self._client.eval(self._RELEASE_SCRIPT, 1, self._key, self._token)  # type: ignore
        return result == 1

    async def __aenter__(self) -> "RedisSpinLock":
        acquired = await self.acquire()
        if not acquired:
            raise TimeoutError(f"Failed to acquire lock '{self._key}'")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()
