from pathlib import Path

import urllib3

from stdl.common.fs_config import read_fs_config_by_file
from stdl.common.types import FsType
from stdl.utils.fs.fs_common_abstract import FsAccessor
from stdl.utils.fs.fs_local import LocalFsAccessor
from stdl.utils.fs.fs_s3 import S3FsAccessor


def create_fs_accessor(fs_conf_path: str, fs_type: FsType) -> FsAccessor:
    if Path(fs_conf_path).exists() is False:
        return LocalFsAccessor()
    fs_conf = read_fs_config_by_file(fs_conf_path)
    if fs_type == FsType.S3:
        if fs_conf.s3.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return S3FsAccessor(fs_conf.s3)
    else:
        return LocalFsAccessor()
