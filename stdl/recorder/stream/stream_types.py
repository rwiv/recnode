from pydantic import BaseModel
from pyutils import error_dict

from ..schema.recording_schema import RecorderStatusInfo, RecordingStatus
from ...fetcher import LiveInfo


class RequestContext(BaseModel):
    stream_url: str
    stream_base_url: str | None
    headers: dict[str, str]
    video_name: str
    tmp_dir_path: str
    out_dir_path: str
    live: LiveInfo

    def to_err(self, ex: Exception, with_stream_url: bool = True) -> dict[str, str]:
        err = error_dict(ex)
        if with_stream_url:
            err["stream_url"] = self.stream_url
        err["video_name"] = self.video_name
        self.live.set_dict(err)
        return err

    def to_dict(self, with_stream_url: bool = False) -> dict[str, str]:
        result = {
            "video_name": self.video_name,
        }
        if with_stream_url:
            result["stream_url"] = self.stream_url
        self.live.set_dict(result)
        return result

    def get_thread_path(self):
        return f"{self.live.platform.value}:{self.live.channel_id}:{self.video_name}"

    def to_status(self, fs_name: str, num: int, status: RecordingStatus) -> RecorderStatusInfo:
        return RecorderStatusInfo(
            platform=self.live.platform,
            channel_id=self.live.channel_id,
            live_id=self.live.live_id,
            fs_name=fs_name,
            video_name=self.video_name,
            num=num,
            status=status,
            stream_url=self.stream_url,
        )
