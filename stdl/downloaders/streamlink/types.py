from abc import abstractmethod
from enum import Enum

from stdl.common.types import PlatformType


class RecordState(Enum):
    WAIT = 0
    RECORDING = 1
    DONE = 2
    FAILED = 3


class AbstractRecorder:
    def __init__(self, uid: str, platform_type: PlatformType):
        self.uid = uid
        self.platform_type = platform_type

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def finish(self):
        pass
