from abc import abstractmethod
from enum import Enum

from stdl.common.types import PlatformType


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


class IRecorder:
    @abstractmethod
    def get_uid(self) -> str:
        pass

    @abstractmethod
    def get_state(self) -> RecordState:
        pass

    @abstractmethod
    def get_platform_type(self) -> PlatformType:
        pass

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def finish(self):
        pass
