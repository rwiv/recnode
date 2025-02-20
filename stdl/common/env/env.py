import os

from pydantic import BaseModel, constr
from pyutils import load_dot_env, path_join, find_project_root

from .env_amqp import AmqpConfig, read_amqp_config
from ..fs import FsType


class Env(BaseModel):
    env: constr(min_length=1)
    fs_type: FsType
    fs_config_path: constr(min_length=1) | None = None
    out_dir_path: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    config_path: constr(min_length=1) | None = None
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

    return Env(
        env=env,
        fs_type=FsType(fs_type),
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        out_dir_path=os.getenv("OUT_DIR_PATH"),
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        config_path=os.getenv("CONFIG_PATH"),
        amqp=read_amqp_config(),
    )
