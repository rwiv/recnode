from pydantic import BaseModel, constr

from ...utils import StreamLinkSessionArgs


class RecordingArgs(BaseModel):
    live_url: str
    session_args: StreamLinkSessionArgs
    tmp_dir_path: constr(min_length=1)
    seg_size_mb: int | None
