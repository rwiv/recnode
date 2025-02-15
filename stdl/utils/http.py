user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"


def get_headers(
        cookies: list[dict] | None = None,
        accept: str | None = None,
) -> dict:
    headers = {
        "User-Agent": user_agent,
    }
    if accept is not None:
        headers["Accept"] = accept
    if cookies is not None:
        headers["Cookie"] = create_cookie_str(cookies)
    return headers


def create_cookie_str(cookies: list[dict]) -> str:
    result = ""
    for i, cookie in enumerate(cookies):
        result += f"{cookie['name']}={cookie['value']}"
        if i != len(cookies)-1:
            result += "; "
    return result
