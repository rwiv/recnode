import os
from dataclasses import dataclass
from typing import Optional
from stdl.platforms.afreeca.types import AfreecaCredential


@dataclass
class Env:
    config_path: str
    afreeca_credential: Optional[AfreecaCredential]


def get_env() -> Env:
    config_path = os.getenv("CONFIG_PATH") or None
    if config_path is None:
        config_path = "../dev/conf.yaml"

    afreeca_username = os.getenv("AFREECA_USERNAME") or None
    afreeca_password = os.getenv("AFREECA_PASSWORD") or None
    afreeca_credential = None
    if afreeca_username is not None and afreeca_password is not None:
        afreeca_credential = AfreecaCredential(username=afreeca_username, password=afreeca_password)

    return Env(
        config_path=config_path,
        afreeca_credential=afreeca_credential,
    )
