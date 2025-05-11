import os
import sys

from .async_types import *
from .http import FIREFOX_USER_AGENT
from .http_async import AsyncHttpClient, AsyncHttpClientMock
from .string import random_string
from .errors import HttpError, HttpRequestError
from .path import *

targets = [
    "async_types",
    "errors",
    "http_async",
    "string",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
