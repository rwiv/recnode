from .live_state import LiveState
from ..redis import RedisMap


KEY_PREFIX = "live"


class LiveStateService:
    def __init__(self, client: RedisMap):
        self.__client = client

    def get(self, record_id: str) -> LiveState | None:
        text = self.__client.get(f"{KEY_PREFIX}:{record_id}")
        if text is None:
            return None
        return LiveState.parse_raw(text)

    def set(self, state: LiveState):
        text = state.model_dump_json(by_alias=True)
        self.__client.set(f"{KEY_PREFIX}:{state.id}", text, nx=True)

    def delete(self, record_id: str):
        self.__client.delete(f"{KEY_PREFIX}:{record_id}")
