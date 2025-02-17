from datetime import datetime

from pydantic import BaseModel


class FileInfo(BaseModel):
    name: str
    path: str
    is_dir: bool
    size: int
    mtime: datetime
