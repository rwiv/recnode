import os

from pydantic import BaseModel, constr
from pyutils import load_dotenv, path_join, find_project_root

from .env_amqp import AmqpConfig, read_amqp_config
from .env_redis import RedisConfig, read_redis_config
from .env_stream import StreamConfig, read_stream_config
from .env_watcher import read_watcher_config, WatcherConfig
from ..spec import LOCAL_FS_NAME


class Env(BaseModel):
    env: constr(min_length=1)
    fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1) | None
    out_dir_path: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    config_path: constr(min_length=1) | None
    stream: StreamConfig
    redis: RedisConfig
    amqp: AmqpConfig
    watcher: WatcherConfig


def get_env() -> Env:
    env = os.getenv("PY_ENV") or None
    if env is None:
        env = "dev"
    if env == "dev":
        load_dotenv(path_join(find_project_root(), "dev", ".env"))

    fs_name = os.getenv("FS_NAME") or None
    if fs_name is None:
        fs_name = LOCAL_FS_NAME

    return Env(
        env=env,
        fs_name=fs_name,
        fs_config_path=os.getenv("FS_CONFIG_PATH") or None,
        out_dir_path=os.getenv("OUT_DIR_PATH"),
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        config_path=os.getenv("CONFIG_PATH") or None,
        stream=read_stream_config(),
        redis=read_redis_config(),
        amqp=read_amqp_config(),
        watcher=read_watcher_config(),
    )
