from abc import ABC, abstractmethod

from stdl.common.types import PlatformType


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
