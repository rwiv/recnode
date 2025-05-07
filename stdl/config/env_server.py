import os

from pydantic import BaseModel, constr, conint
from pyutils import load_dotenv, path_join, find_project_root

from .config_proxy import ProxyConfig, read_proxy_config
from .config_redis import RedisConfig, read_redis_config
from .config_request import RequestConfig, read_request_config
from .config_stream import StreamConfig, read_stream_config
from ..common.spec import LOCAL_FS_NAME


class Env(BaseModel):
    env: constr(min_length=1)
    port: conint(ge=0)
    fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1) | None
    out_dir_path: constr(min_length=1) | None
    tmp_dir_path: constr(min_length=1)
    config_path: constr(min_length=1) | None
    req_conf: RequestConfig
    stream: StreamConfig
    redis: RedisConfig
    proxy: ProxyConfig


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
        port=os.getenv("SERVER_PORT") or 9083,  # type: ignore
        fs_name=fs_name,
        fs_config_path=os.getenv("FS_CONFIG_PATH") or None,
        out_dir_path=os.getenv("OUT_DIR_PATH") or None,
        tmp_dir_path=os.getenv("TMP_DIR_PATH"),
        config_path=os.getenv("CONFIG_PATH") or None,
        req_conf=read_request_config(),
        stream=read_stream_config(),
        redis=read_redis_config(),
        proxy=read_proxy_config(),
    )
