import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Env:
    out_dir_path: str
    tmp_dir_path: str
    config_path: Optional[str]


def get_env() -> Env:
    out_dir_path = os.getenv("OUT_DIR_PATH") or None
    if out_dir_path is None:
        raise ValueError("OUT_DIR_PATH is not set")
    tmp_dir_path = os.getenv("TMP_DIR_PATH") or None
    if tmp_dir_path is None:
        raise ValueError("TMP_DIR_PATH is not set")
    config_path = os.getenv("CONFIG_PATH") or None

    return Env(
        out_dir_path=out_dir_path,
        tmp_dir_path=tmp_dir_path,
        config_path=config_path,
    )
