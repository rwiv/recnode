import os
import sys

from .fs_config import FsConfigYaml, S3Config
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter
from .object_writer_utils import read_fs_config_by_file, create_fs_writer, create_proxy_fs_writer
from .fs_types import FsType

targets = ["fs_configs", "fs_constants", "fs_types", "object_writer", "object_writer_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
