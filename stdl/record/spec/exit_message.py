from enum import Enum

from pydantic import BaseModel

from ...common.spec import PlatformType


class ExitCommand(Enum):
    CANCEL = "cancel"
    FINISH = "finish"


class ExitMessage(BaseModel):
    cmd: ExitCommand
    platform: PlatformType
    uid: str
