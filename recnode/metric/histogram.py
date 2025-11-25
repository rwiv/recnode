import asyncio
import bisect


class Histogram:
    def __init__(self, buckets: list[float]):
        if buckets[-1] != float("inf"):
            buckets = buckets + [float("inf")]
        self.__lock = asyncio.Lock()
        self.buckets = buckets
        self.hist: dict[float, float] = {}
        for bucket in self.buckets:
            self.hist[bucket] = 0
        self.total_count = 0
        self.total_sum = 0.0

    async def observe(self, value: float):
        async with self.__lock:
            key = bisect.bisect_left(self.buckets, value)
            self.hist[self.buckets[key]] += 1
            self.total_count += 1
            self.total_sum += value

    def avg(self):
        if self.total_count == 0:
            return 0
        return self.total_sum / self.total_count
