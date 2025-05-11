def stem(name: str):
    i = name.rfind(".")
    if 0 < i < len(name) - 1:
        return name[:i]
    else:
        return name
