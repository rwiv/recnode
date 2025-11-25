from .fs_config import FsConfig
from .fs_types import FsType
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter, ProxyObjectWriter
from ..common import LOCAL_FS_NAME
from ..config import ProxyServerConfig


def create_fs_writer(fs_name: str, configs: list[FsConfig], proxy_server: ProxyServerConfig) -> ObjectWriter:
    if proxy_server.enabled:
        if proxy_server.endpoint is None:
            raise ValueError("Proxy endpoint is not set")
        return ProxyObjectWriter(proxy_server.endpoint, fs_name)

    if fs_name == LOCAL_FS_NAME:
        return LocalObjectWriter()

    fs_conf = None
    for conf in configs:
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"Cannot find configuration with name {fs_name}")

    if fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"Cannot find S3 configuration with name {fs_name}")
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3)

    raise ValueError(f"Unsupported fs_name: {fs_name}")


def create_proxy_fs_writer(fs_name: str, configs: list[FsConfig]) -> ObjectWriter:
    fs_conf = None
    for conf in configs:
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"Cannot find configuration with name {fs_name}")

    if fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"Cannot find S3 configuration with name {fs_name}")
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3)

    raise ValueError(f"Unsupported fs_name: {fs_name}")
