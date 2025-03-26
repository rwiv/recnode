import os

from pydantic import BaseModel, constr, conint
from pyutils import load_dotenv, path_join, find_project_root

from .env_amqp import AmqpConfig, read_amqp_config
from .env_watcher import read_watcher_config, WatcherConfig
from ..common import LOCAL_FS_NAME


class Env(BaseModel):
    env: constr(min_length=1)
    fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1) | None = None
    out_dir_path: constr(min_length=1)
    tmp_dir_path: constr(min_length=1)
    config_path: constr(min_length=1) | None = None
    seg_size_mb: conint(ge=1) | None = None
    amqp: AmqpConfig
    use_watcher: bool
    watcher: WatcherConfig | None = None


def get_env() -> Env:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"
    if env == "dev":
        load_dotenv(path_join(find_project_root(), "dev", ".env"))

    fs_name = os.getenv("FS_NAME")
    if fs_name is None:
        fs_name = LOCAL_FS_NAME

    use_watcher = os.getenv("USE_WATCHER") == "true"
    watcher_conf = None
    if use_watcher:
        watcher_conf = read_watcher_config()

    return Env(
        env=env,
        fs_name=fs_name,
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        out_dir_path=os.getenv("OUT_DIR_PATH"),
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        config_path=os.getenv("CONFIG_PATH"),
        seg_size_mb=os.getenv("SEG_SIZE_MB"),
        amqp=read_amqp_config(),
        use_watcher=use_watcher,
        watcher=watcher_conf,
    )
