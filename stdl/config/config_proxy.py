import os

from pydantic import BaseModel, constr


class ProxyConfig(BaseModel):
    enabled: bool
    endpoint: constr(min_length=1) | None


def read_proxy_config() -> ProxyConfig:
    return ProxyConfig(
        enabled=os.getenv("PROXY_ENABLED") == "true",
        endpoint=os.getenv("PROXY_ENDPOINT") or None,
    )
