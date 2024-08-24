import os
from dataclasses import dataclass


@dataclass
class Env:
    config_path: str


def get_env() -> Env:
    config_path = os.getenv("CONFIG_PATH") or None
    if config_path is None:
        config_path = "../dev/conf.yaml"

    return Env(
        config_path=config_path,
    )
