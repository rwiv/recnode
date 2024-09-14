from typing import Optional

from stdl.utils.http import create_cookie_str

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


def get_headers(cookies: list[dict], accept: Optional[str] = None) -> dict:
    headers = {
        "User-Agent": user_agent,
    }
    if accept is not None:
        headers["Accept"] = accept
    if cookies is not None:
        headers["Cookie"] = create_cookie_str(cookies)
    return headers
