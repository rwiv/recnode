from abc import abstractmethod
from enum import Enum


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


class IRecorder:
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_state(self) -> RecordState:
        pass

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def finish(self):
        pass
