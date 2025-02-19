from pathlib import Path

import urllib3

from pynifs import FsAccessor, LocalFsAccessor, S3FsAccessor

from .fs_config import read_fs_config_by_file
from ..env import Env
from ..request import AppConfig
from ..spec import FsType


def create_fs_accessor(env: Env, conf: AppConfig) -> FsAccessor:
    if env.fs_config_path is not None and conf.fs_type is not None:
        return __create_fs_accessor(env.fs_config_path, conf.fs_type)
    else:
        return LocalFsAccessor()


def __create_fs_accessor(fs_conf_path: str, fs_type: FsType) -> FsAccessor:
    if Path(fs_conf_path).exists() is False:
        return LocalFsAccessor()
    fs_conf = read_fs_config_by_file(fs_conf_path)
    if fs_type == FsType.S3:
        if fs_conf.s3.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return S3FsAccessor(fs_conf.s3)
    else:
        return LocalFsAccessor()
