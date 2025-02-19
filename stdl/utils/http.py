from pyutils import cookie_header, CookieDict

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


def get_headers(
    cookies: list[CookieDict] | None = None,
    accept: str | None = None,
) -> dict:
    headers = {
        "User-Agent": user_agent,
    }
    if accept is not None:
        headers["Accept"] = accept
    if cookies is not None:
        headers["Cookie"] = cookie_header(cookies)
    return headers
