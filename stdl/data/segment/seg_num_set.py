from stdl.data.redis import RedisSortedSet


class SegmentNumberSet:
    def __init__(self, sorted_set: RedisSortedSet, key_suffix: str):
        self.__sorted_set = sorted_set
        self.__key_suffix = key_suffix

    async def add(self, live_record_id: str, num: int):
        await self.__sorted_set.set(self.__get_key(live_record_id), str(num), num)

    async def all(self, live_record_id: str) -> list[int]:
        result = await self.__sorted_set.list(self.__get_key(live_record_id))
        return [int(i) for i in result]

    async def range(self, live_record_id: str, start: int, end: int) -> list[int]:
        result = await self.__sorted_set.range_by_score(self.__get_key(live_record_id), start, end)
        return [int(i) for i in result]

    async def remove(self, live_record_id: str, num: int):
        await self.__sorted_set.remove_by_score(self.__get_key(live_record_id), num, num)

    async def contains(self, live_record_id: str, num: int) -> bool:
        return await self.__sorted_set.contains_by_score(self.__get_key(live_record_id), num)

    async def size(self, live_record_id: str) -> int:
        return await self.__sorted_set.size(self.__get_key(live_record_id))

    async def clear(self, live_record_id: str):
        await self.__sorted_set.clear(self.__get_key(live_record_id))

    def __get_key(self, live_record_id: str):
        return f"live:{live_record_id}:seg:{self.__key_suffix}"
