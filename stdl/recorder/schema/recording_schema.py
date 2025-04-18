from enum import Enum

from pydantic import BaseModel, Field
from pyutils import log

from ...common.spec import PlatformType


class StreamInfo(BaseModel):
    uid: str
    url: str
    platform: PlatformType


class RecordingState(BaseModel):
    abort_flag: bool = False
    cancel_flag: bool = False

    def cancel(self):
        log.info("Cancel Request")
        self.abort_flag = True
        self.cancel_flag = True

    def finish(self):
        log.info("Finish Request")
        self.abort_flag = True


class RecordingStatus(Enum):
    WAIT = "wait"
    RECORDING = "recording"
    DONE = "done"
    FAILED = "failed"


class RecorderStatusInfo(BaseModel):
    platform: PlatformType
    channel_id: str = Field(serialization_alias="channelId")
    live_id: str = Field(serialization_alias="liveId")
    num: int
    stream_status: RecordingStatus = Field(serialization_alias="streamStatus")
