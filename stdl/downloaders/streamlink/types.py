from abc import abstractmethod, ABC
from enum import Enum

from stdl.common.types import PlatformType


class RecordState(Enum):
    WAIT = "wait"
    RECORDING = "recording"
    DONE = "done"
    FAILED = "failed"


class AbstractRecorder(ABC):
    def __init__(self, uid: str, platform_type: PlatformType):
        self.uid = uid
        self.platform_type = platform_type

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def finish(self):
        pass
