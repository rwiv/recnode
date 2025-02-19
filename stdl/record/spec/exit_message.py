from enum import Enum

from pydantic import BaseModel

from ...common import PlatformType


class ExitCommand(Enum):
    CANCEL = "cancel"
    FINISH = "finish"


class ExitMessage(BaseModel):
    cmd: ExitCommand
    platform: PlatformType
    uid: str
