import asyncio


class AsyncSet[T]:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.set = set()

    async def add(self, item):
        async with self.lock:
            self.set.add(item)

    async def remove(self, item):
        async with self.lock:
            if item in self.set:
                self.set.remove(item)

    def contains(self, item) -> bool:
        return item in self.set

    def list(self):
        return list(self.set)


class AsyncCounter:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.count = 0

    async def increment(self):
        async with self.lock:
            self.count += 1

    async def decrement(self):
        async with self.lock:
            if self.count > 0:
                self.count -= 1

    async def reset(self):
        async with self.lock:
            self.count = 0

    def get(self) -> int:
        return self.count


class AsyncMap[K, V]:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.map = {}

    async def set(self, key: K, value: V):
        async with self.lock:
            self.map[key] = value

    async def get(self, key: K) -> V | None:
        async with self.lock:
            return self.map.get(key)

    async def remove(self, key: K):
        async with self.lock:
            if key in self.map:
                del self.map[key]

    def contains(self, key: K) -> bool:
        return key in self.map

    def keys(self) -> list[K]:
        return list(self.map.keys())

    def items(self):
        return self.map.items()
