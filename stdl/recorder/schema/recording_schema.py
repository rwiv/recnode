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
    id: str
    platform: PlatformType
    channel_id: str = Field(serialization_alias="channelId")
    channel_name: str = Field(serialization_alias="channelName")
    live_id: str = Field(serialization_alias="liveId")
    video_name: str = Field(serialization_alias="videoName")
    fs_name: str = Field(serialization_alias="fsName")
    num: int
    status: RecordingStatus = Field(serialization_alias="status")
    stream_url: str = Field(serialization_alias="streamUrl")
