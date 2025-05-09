import asyncio
import uuid
import random
import redis.asyncio as redis
from asyncio import timeout

from pyutils import log

_RELEASE_SCRIPT = r"""
-- KEYS[1] = lock_key, KEYS[2] = channel
-- ARGV[1] = token, ARGV[2] = expire_ms
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
    -- owned by caller: delete and notify
    redis.call('DEL', KEYS[1])
    redis.call('PUBLISH', KEYS[2], 'unlock')
    return 1
elseif not v then
    -- already expired: notify
    redis.call('PUBLISH', KEYS[2], 'unlock')
    return 2
else
    -- owned by someone else
    return 0
end
"""

_RENEW_SCRIPT = r"""
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
        retry_delay_enabled: bool = False,
        auto_renew_enabled: bool = False,
        auto_renew_interval_sec: float | None = None,
    ):
        self.__client = client
        self.__key = key
        self.__channel = f"__lock_channel__:{key}"
        self.__expire_ms = expire_ms
        self.__timeout_sec = timeout_sec
        self.__retry_delay_enabled = retry_delay_enabled
        self.__auto_renew_enabled = auto_renew_enabled
        self.__auto_renew_interval = auto_renew_interval_sec or (expire_ms / 1000 * 0.5)
        self.__token = str(uuid.uuid4())

        self.__locked = False
        self.__renew_task = None

    async def acquire(self) -> bool:
        loop = asyncio.get_running_loop()
        start = loop.time()

        # initial lock attempt
        if await self.__client.set(self.__key, self.__token, nx=True, px=self.__expire_ms):
            self.__locked = True
            if self.__auto_renew_enabled:
                self.__start_auto_renew()
            return True

        # failed initial, subscribe for notifications
        sub = self.__client.pubsub()
        await sub.subscribe(self.__channel)

        try:
            while True:
                elapsed = loop.time() - start
                remaining = self.__timeout_sec - elapsed
                if remaining <= 0:
                    return False

                # retry lock acquisition
                if await self.__client.set(self.__key, self.__token, nx=True, px=self.__expire_ms):
                    self.__locked = True
                    if self.__auto_renew_enabled:
                        self.__start_auto_renew()
                    return True

                # wait for unlock signal or timeout
                try:
                    async with timeout(remaining):
                        async for msg in sub.listen():
                            if msg.get("type") == "message":
                                break
                except asyncio.TimeoutError:
                    return False

                # random jitter to avoid thundering herd
                if self.__retry_delay_enabled:
                    await asyncio.sleep(random.uniform(0, 0.1))
        finally:
            await sub.unsubscribe(self.__channel)
            await sub.close()

    async def release(self) -> None:
        if not self.__locked:
            raise ValueError("Lock not acquired")
        if self.__renew_task is not None:
            await self.__stop_auto_renew()
        await self.__client.eval(_RELEASE_SCRIPT, 2, self.__key, self.__channel, self.__token)  # type: ignore
        self.__locked = False

    async def renew(self) -> bool:
        if not self.__locked:
            raise ValueError("Lock not acquired")
        result = await self.__client.eval(_RENEW_SCRIPT, 1, self.__key, self.__token, self.__expire_ms)  # type: ignore
        return bool(result)

    async def __aenter__(self):
        if not await self.acquire():
            log.error("Lock acquisition timeout")  # TODO: check log
            raise asyncio.TimeoutError("Lock acquisition timeout")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    async def __auto_renew_loop(self):
        try:
            while self.__locked:
                await asyncio.sleep(self.__auto_renew_interval)
                if not await self.renew():
                    break
        except asyncio.CancelledError:
            pass

    def __start_auto_renew(self):
        if not self.__locked:
            raise RuntimeError("Lock not acquired")
        self.__renew_task = asyncio.create_task(self.__auto_renew_loop())

    async def __stop_auto_renew(self):
        if self.__renew_task is not None:
            self.__renew_task.cancel()
            try:
                await self.__renew_task
            except asyncio.CancelledError:
                pass
            self.__renew_task = None
