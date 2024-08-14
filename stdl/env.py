import os
from typing import TypedDict, Optional


class Env(TypedDict):
    uid: Optional[str]
    out_dir: Optional[str]
    cookies: Optional[str]


def get_env():
    return {
        "uid": os.getenv("CHZZK_UID") or None,
        "out_dir": os.getenv("OUT_DIR") or None,
        "cookies": os.getenv("HTTP_COOKIES") or None,
    }
