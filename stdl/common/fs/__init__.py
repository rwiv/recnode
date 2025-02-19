import os
import sys

from .fs_config import FsConfig
from .fs_config_utils import read_fs_config_by_file, create_fs_accessor

targets = ["fs_configs", "fs_configs_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
