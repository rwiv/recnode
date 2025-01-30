from enum import Enum

from pydantic import BaseModel

from stdl.common.types import PlatformType


class ExitCommand(Enum):
    DELETE = "delete"
    CANCEL = "cancel"


class ExitMessage(BaseModel):
    cmd: ExitCommand
    platform: PlatformType
    uid: str
