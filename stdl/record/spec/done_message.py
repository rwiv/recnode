from enum import Enum

from pydantic import BaseModel

from ...common.spec import PlatformType, FsType


class DoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class DoneMessage(BaseModel):
    status: DoneStatus
    ptype: PlatformType
    uid: str
    vidname: str
    fstype: FsType
