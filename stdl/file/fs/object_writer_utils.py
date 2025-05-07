from pathlib import Path

from .fs_config import read_fs_config_by_file
from .fs_types import FsType
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter, ProxyObjectWriter
from ..s3.s3_utils import disable_warning_log
from ...config import Env, ProxyEnv


def create_fs_writer(env: Env) -> ObjectWriter:
    if env.proxy.enabled:
        if env.proxy.endpoint is None:
            raise ValueError("Proxy endpoint is not set")
        return ProxyObjectWriter(env.proxy.endpoint, env.fs_name)

    fs_name = env.fs_name
    fs_conf_path = env.fs_config_path

    if fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalObjectWriter()
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
        if not fs_conf.s3.verify:
            disable_warning_log()
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3)
    else:
        return LocalObjectWriter()


def create_proxy_fs_writer(env: ProxyEnv) -> ObjectWriter:
    fs_name = env.fs_name
    fs_conf_path = env.fs_config_path

    if fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalObjectWriter()
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
        if not fs_conf.s3.verify:
            disable_warning_log()
        return S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3)
    else:
        return LocalObjectWriter()
