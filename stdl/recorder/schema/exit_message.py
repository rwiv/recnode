from enum import Enum

from pydantic import BaseModel, constr

from ...common import PlatformType


class ExitCommand(Enum):
    CANCEL = "cancel"
    FINISH = "finish"


class ExitMessage(BaseModel):
    cmd: ExitCommand
    platform: PlatformType
    uid: constr(min_length=1)
