from pathlib import Path
from typing import Any


def find_project_root():
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / ".git").exists():
            return parent
    return current_path


def dirname(file_path: str, delimiter: str = "/") -> str:
    return delimiter.join(file_path.split(delimiter)[:-1])


def path_join(*paths: Any, delimiter: str = "/") -> str:
    return delimiter.join(str(p).strip(delimiter) for p in paths if p)
