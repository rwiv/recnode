from recnode.utils import HttpError


class RedisError(HttpError):
    def __init__(self, message: str, status: int = 500):
        super().__init__(status=status, message=message)
