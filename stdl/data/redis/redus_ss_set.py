from redis.asyncio import Redis


class RedisSingleSortedSet:
    def __init__(self, client: Redis):
        self.__redis = client
