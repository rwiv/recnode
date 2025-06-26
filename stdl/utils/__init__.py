import os
import sys

from .async_types import *
from .http import FIREFOX_USER_AGENT, fetch_my_public_ip
from .http_async import AsyncHttpClient, AsyncHttpClientMock
from .errors import HttpError, HttpRequestError
from .path import *
from .streamlink import *
from .string import random_string

targets = [
    "async_types",
    "errors",
    "http",
    "http_async",
    "path",
    "streamlink",
    "string",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
