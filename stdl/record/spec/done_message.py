from enum import Enum

from pydantic import BaseModel
from pynifs import FsType

from ...common.spec import PlatformType


class DoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class DoneMessage(BaseModel):
    status: DoneStatus
    ptype: PlatformType
    uid: str
    vidname: str
    fstype: FsType
