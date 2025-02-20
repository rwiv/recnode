import os

from pydantic import BaseModel
from pynifs import FsType
from pyutils import load_dot_env, path_join, find_project_root

from .env_amqp import AmqpConfig, read_amqp_config


class Env(BaseModel):
    env: str
    fs_type: FsType
    fs_config_path: str | None
    out_dir_path: str
    tmp_dir_path: str
    config_path: str | None
    amqp: AmqpConfig


def get_env() -> Env:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"
    if env == "dev":
        load_dot_env(path_join(find_project_root(), "dev", ".env"))
    
    fs_type = os.getenv("FS_TYPE")
    if fs_type is None:
        raise ValueError("FS_TYPE is not set")

    fs_config_path = os.getenv("FS_CONFIG_PATH")
    out_dir_path = os.getenv("OUT_DIR_PATH")
    if out_dir_path is None:
        raise ValueError("OUT_DIR_PATH is not set")
    tmp_dir_path = os.getenv("TMP_DIR_PATH")
    if tmp_dir_path is None:
        raise ValueError("TMP_DIR_PATH is not set")
    config_path = os.getenv("CONFIG_PATH")

    return Env(
        env=env,
        fs_type=FsType(fs_type),
        fs_config_path=fs_config_path,
        out_dir_path=out_dir_path,
        tmp_dir_path=tmp_dir_path,
        config_path=config_path,
        amqp=read_amqp_config(),
    )
