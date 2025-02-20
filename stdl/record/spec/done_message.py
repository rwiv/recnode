from enum import Enum

from pydantic import BaseModel, Field, constr
from pynifs import FsType

from ...common.spec import PlatformType


class DoneStatus(Enum):
    COMPLETE = "complete"
    CANCELED = "canceled"


class DoneMessage(BaseModel):
    status: DoneStatus
    platform: PlatformType
    uid: constr(min_length=1)
    video_name: constr(min_length=1) = Field(serialization_alias="videoName")
    fs_type: FsType = Field(serialization_alias="fsType")
