import os

from pydantic import BaseModel, constr
from pyutils import load_dotenv, path_join, find_project_root

from .env_proxy import ProxyConfig, read_proxy_config
from ..spec import LOCAL_FS_NAME


class ProxyEnv(BaseModel):
    env: constr(min_length=1)
    fs_name: constr(min_length=1)
    fs_config_path: constr(min_length=1) | None
    proxy: ProxyConfig


def get_proxy_env() -> ProxyEnv:
    env = os.getenv("PY_ENV") or None
    if env is None:
        env = "dev"
    if env == "dev":
        load_dotenv(path_join(find_project_root(), "dev", ".env"))

    fs_name = os.getenv("FS_NAME") or None
    if fs_name is None:
        fs_name = LOCAL_FS_NAME

    return ProxyEnv(
        env=env,
        fs_name=fs_name,
        fs_config_path=os.getenv("FS_CONFIG_PATH") or None,
        proxy=read_proxy_config(),
    )
