def create_cookie_str(cookies: list[dict]) -> str:
    result = ""
    for i, cookie in enumerate(cookies):
        result += f"{cookie["name"]}={cookie["value"]}"
        if i != len(cookies)-1:
            result += "; "
    return result
