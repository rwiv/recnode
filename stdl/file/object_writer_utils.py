from pathlib import Path

from .fs_config import read_fs_config_by_file
from .fs_types import FsType
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter, ProxyObjectWriter
from ..config import Env, ProxyEnv
from ..metric import MetricManager


def create_fs_writer(env: Env, metric: MetricManager) -> ObjectWriter:
    if env.proxy.enabled:
        if env.proxy.endpoint is None:
            raise ValueError("Proxy endpoint is not set")
        return ProxyObjectWriter(env.proxy.endpoint, env.fs_name, metric)

    fs_name = env.fs_name
    fs_conf_path = env.fs_config_path

    if fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalObjectWriter(metric=metric)
    fs_conf = None
    for conf in read_fs_config_by_file(fs_conf_path):
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"Cannot find configuration with name {fs_name}")

    if fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"Cannot find S3 configuration with name {fs_name}")
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3, metric=metric)
    else:
        return LocalObjectWriter(metric=metric)


def create_proxy_fs_writer(env: ProxyEnv, metric: MetricManager) -> ObjectWriter:
    fs_name = env.fs_name
    fs_conf_path = env.fs_config_path

    if fs_conf_path is None or not Path(fs_conf_path).exists():
        raise ValueError("File system configuration path is not set")
    fs_conf = None
    for conf in read_fs_config_by_file(fs_conf_path):
        if conf.name == fs_name:
            fs_conf = conf
            break
    if fs_conf is None:
        raise ValueError(f"Cannot find configuration with name {fs_name}")

    if fs_conf.type == FsType.S3:
        if fs_conf.s3 is None:
            raise ValueError(f"Cannot find S3 configuration with name {fs_name}")
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3, metric=metric)
    else:
        raise ValueError(f"Unsupported file system type {fs_conf.type} for proxy writer")
