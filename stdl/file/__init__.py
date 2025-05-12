import os
import sys

from .fs_config import FsConfig
from .object_writer import ObjectWriter, S3ObjectWriter, LocalObjectWriter, ProxyObjectWriter
from .object_writer_utils import read_fs_config_by_file, create_fs_writer, create_proxy_fs_writer
from .fs_types import FsType
from .s3_utils import create_async_client

targets = [
    "fs_config",
    "object_writer",
    "object_writer_utils",
    "fs_types",
    "s3_utils",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
