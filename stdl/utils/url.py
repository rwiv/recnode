from urllib.parse import urlparse, parse_qs


def get_base_url(url: str) -> str:
    parsed_rul = urlparse(url)
    new_path = parsed_rul.path.rsplit('/', 1)[0]
    return f"{parsed_rul.scheme}://{parsed_rul.netloc}{new_path}"


def get_origin(url: str) -> str:
    parsed_rul = urlparse(url)
    return f"{parsed_rul.scheme}://{parsed_rul.netloc}"


def get_query_string(url: str) -> str:
    parsed_rul = urlparse(url)
    return parsed_rul.query


def find_query_value_one(url: str, key: str) -> str:
    parsed_rul = urlparse(url)
    params = parse_qs(parsed_rul.query)
    values = params[key]
    if len(values) != 1:
        raise ValueError("query values should be 1")
    return values[0]
