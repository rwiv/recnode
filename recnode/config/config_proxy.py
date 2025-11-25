import os

from pydantic import BaseModel, conint, constr


class ProxyServerConfig(BaseModel):
    enabled: bool
    endpoint: constr(min_length=1) | None


def read_proxy_server_config() -> ProxyServerConfig:
    return ProxyServerConfig(
        enabled=os.getenv("PROXY_ENABLED") == "true",
        endpoint=os.getenv("PROXY_ENDPOINT") or None,
    )


class ProxyConfig(BaseModel):
    host: constr(min_length=1) | None
    port_domestic: conint(ge=0)
    port_overseas: conint(ge=0)
    username: constr(min_length=1)
    password: constr(min_length=1)
    rdns: bool
    use_my_ip: bool


def read_proxy_config() -> ProxyConfig | None:
    host = os.getenv("PROXY_HOST") or None
    port_domestic = os.getenv("PROXY_PORT_DOMESTIC") or None
    port_overseas = os.getenv("PROXY_PORT_OVERSEAS") or None
    username = os.getenv("PROXY_USERNAME") or None
    password = os.getenv("PROXY_PASSWORD") or None

    checklist = [port_domestic, port_overseas, username, password]
    if any(item is None for item in checklist):
        return None

    return ProxyConfig(
        host=host,
        port_domestic=port_domestic,
        port_overseas=port_overseas,
        username=username,
        password=password,
        rdns=os.getenv("PROXY_RDNS") == "true",
        use_my_ip=os.getenv("PROXY_USE_MY_IP") == "true",
    )
