import os

from pydantic import BaseModel, constr, conint
from pyutils import load_dotenv, path_join, find_project_root

from .config_proxy import ProxyServerConfig, read_proxy_server_config


class ProxyEnv(BaseModel):
    env: constr(min_length=1)
    port: conint(ge=0)
    fs_config_path: constr(min_length=1)
    proxy: ProxyServerConfig


def get_proxy_env() -> ProxyEnv:
    env = os.getenv("PY_ENV") or None
    if env is None:
        env = "dev"
    if env == "dev":
        load_dotenv(path_join(find_project_root(), "dev", ".env-proxy"))

    return ProxyEnv(
        env=env,
        port=os.getenv("SERVER_PORT") or 9084,  # type: ignore
        fs_config_path=os.getenv("FS_CONFIG_PATH"),
        proxy=read_proxy_server_config(),
    )
