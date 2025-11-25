from pathlib import Path


def stem(path: str):
    return Path(path).stem
