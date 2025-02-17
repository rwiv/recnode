def to_dir_path(path: str):
    result = path.lstrip("/")
    if result == "":
        return ""
    if result[-1] == "/":
        return result
    else:
        return result + "/"
