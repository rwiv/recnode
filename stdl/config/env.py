import os
from dataclasses import dataclass
from typing import Optional
from stdl.platforms.afreeca.types import AfreecaCredential


@dataclass
class Env:
    out_dir_path: str
    tmp_dir_path: str
    config_path: Optional[str]
    afreeca_credential: Optional[AfreecaCredential]


def get_env() -> Env:
    out_dir_path = os.getenv("OUT_DIR_PATH") or None
    if out_dir_path is None:
        raise ValueError("OUT_DIR_PATH is not set")
    tmp_dir_path = os.getenv("TMP_DIR_PATH") or None
    if tmp_dir_path is None:
        raise ValueError("TMP_DIR_PATH is not set")
    config_path = os.getenv("CONFIG_PATH") or None

    afreeca_username = os.getenv("AFREECA_USERNAME") or None
    afreeca_password = os.getenv("AFREECA_PASSWORD") or None
    afreeca_credential = None
    if afreeca_username is not None and afreeca_password is not None:
        afreeca_credential = AfreecaCredential(username=afreeca_username, password=afreeca_password)

    return Env(
        out_dir_path=out_dir_path,
        tmp_dir_path=tmp_dir_path,
        config_path=config_path,
        afreeca_credential=afreeca_credential,
    )
