import os
import sys

from .fs_config import FsConfig, S3Config
from .fs_accessor import FsAccessor, LocalFsAccessor, S3FsAccessor
from .fs_accessor_utils import read_fs_config_by_file, create_fs_accessor
from .fs_types import FsType

targets = ["fs_configs", "fs_types", "fs_accessor", "fs_accessor_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
