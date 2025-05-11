import os
import sys

from .fs.fs_config import FsConfig
from .fs.object_writer_async import AsyncObjectWriter, S3AsyncObjectWriter, LocalAsyncObjectWriter
from .fs.object_writer_utils import read_fs_config_by_file, create_fs_writer, create_proxy_fs_writer
from .fs.fs_types import FsType
from .s3.s3_async_utils import create_async_client

targets = ["fs", "s3"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
