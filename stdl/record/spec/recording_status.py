from enum import Enum

from pydantic import BaseModel, Field

from stdl.common.types import PlatformType


class RecordingState(Enum):
    WAIT = "wait"
    RECORDING = "recording"
    DONE = "done"
    FAILED = "failed"


class RecorderStatus(BaseModel):
    platform: PlatformType
    uid: str
    idx: int
    stream_status: RecordingState = Field(alias="streamStatus")
