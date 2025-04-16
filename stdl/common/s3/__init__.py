import os
import sys

from .s3_utils import create_client, disable_warning_log

targets = ["s3_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
