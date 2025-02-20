import os

from pydantic import BaseModel, Field
from pynifs import FsType
from pyutils import load_dot_env, path_join, find_project_root

from .env_amqp import AmqpConfig, read_amqp_config


class Env(BaseModel):
    env: str = Field(min_length=1)
    fs_type: FsType
    fs_config_path: str | None = Field(min_length=1, default=None)
    out_dir_path: str = Field(min_length=1)
    tmp_dir_path: str
    config_path: str | None = Field(min_length=1, default=None)
    amqp: AmqpConfig


def get_env() -> Env:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"
    if env == "dev":
        load_dot_env(path_join(find_project_root(), "dev", ".env"))

    fs_type = os.getenv("FS_TYPE")
    if fs_type is None:
        fs_type = FsType.LOCAL

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
