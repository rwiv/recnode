from enum import Enum

from pydantic import BaseModel
from pynifs import FsType

from ...common.spec import PlatformType


class DoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class DoneMessage(BaseModel):
    status: DoneStatus
    platform: PlatformType
    uid: str
    video_name: str
    fs_type: FsType
