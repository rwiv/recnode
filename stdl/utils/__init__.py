import os
import sys

from .http import FIREFOX_USER_AGENT
from .http_async import AsyncHttpClient
from .string import random_string
from .errors import HttpError, HttpRequestError

targets = [
    "http_async",
    "string",
    "errors",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
