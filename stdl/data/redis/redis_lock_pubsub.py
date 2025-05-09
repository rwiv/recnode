import asyncio
import uuid
import redis.asyncio as redis
from asyncio import timeout


_RELEASE_SCRIPT = """
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
    redis.call('DEL', KEYS[1])
    redis.call('PUBLISH', KEYS[2], 'unlock')
    return 1
elseif not v then
    redis.call('PUBLISH', KEYS[2], 'unlock')
    return 2
else
    return 0
end
"""

_RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
"""


class RedisPubSubLock:

    def __init__(
        self,
        client: redis.Redis,
        key: str,
        expire_ms: int,
        timeout_sec: float,
        auto_renew_enabled: bool = False,
        auto_renew_interval_sec: float | None = None,
    ):
        self.__client = client

        self.__key = key
        self.__channel = f"__lock_channel__:{key}"

        self.__expire_ms = expire_ms
        self.__timeout_sec = timeout_sec

        self.__auto_renew_enabled = auto_renew_enabled
        self.__auto_renew_interval_sec = auto_renew_interval_sec

        self.__token = str(uuid.uuid4())
        self.__sub = client.pubsub()

        self.__locked = False
        self.__renew_task = None

    async def acquire(self) -> bool:
        loop = asyncio.get_running_loop()
        start = loop.time()

        while True:
            ok = await self.__client.set(self.__key, self.__token, nx=True, px=self.__expire_ms)
            if ok:
                self.__locked = True
                if (
                    self.__auto_renew_enabled
                    and self.__auto_renew_interval_sec is not None
                    and self.__auto_renew_interval_sec > 0
                ):
                    self.__start_auto_renew(self.__auto_renew_interval_sec)
                return True

            elapsed = loop.time() - start
            remaining = self.__timeout_sec - elapsed
            if remaining <= 0:
                return False

            await self.__sub.subscribe(self.__channel)
            try:
                if not await self.__client.exists(self.__key):
                    continue

                try:
                    async with timeout(remaining):
                        async for msg in self.__sub.listen():
                            if msg.get("type") == "message":
                                break
                except asyncio.TimeoutError:
                    return False
            finally:
                await self.__sub.unsubscribe(self.__channel)
                await self.__sub.reset()

    async def release(self) -> None:
        if not self.__locked:
            raise ValueError("Lock not acquired")
        self.__stop_auto_renew()
        await self.__client.eval(_RELEASE_SCRIPT, 2, self.__key, self.__channel, self.__token)  # type: ignore
        self.__locked = False

    async def __aenter__(self):
        if not await self.acquire():
            raise asyncio.TimeoutError("Lock acquisition timeout")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    async def renew(self) -> bool:
        if not self.__locked:
            raise ValueError("Lock not acquired")
        result = await self.__client.eval(_RENEW_SCRIPT, 1, self.__key, self.__token, self.__expire_ms)  # type: ignore
        return result == 1

    async def __auto_renew(self, interval_sec: float):
        try:
            while self.__locked:
                await asyncio.sleep(interval_sec)
                success = await self.renew()
                if not success:
                    break
        except asyncio.CancelledError:
            pass

    def __start_auto_renew(self, interval_sec: float):
        if not self.__locked:
            raise RuntimeError("Lock not acquired")
        self.__renew_task = asyncio.create_task(self.__auto_renew(interval_sec))

    def __stop_auto_renew(self):
        if self.__renew_task:
            self.__renew_task.cancel()
