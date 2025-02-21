from pathlib import Path

from .fs_config import read_fs_config_by_file
from .fs_types import FsType
from .fs_accessor import FsAccessor, LocalFsAccessor, S3FsAccessor
from ...utils.s3_utils import disable_warning_log


def create_fs_accessor(fs_type: FsType, fs_conf_path: str | None) -> FsAccessor:
    if fs_type == FsType.LOCAL or fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalFsAccessor()
    fs_conf = read_fs_config_by_file(fs_conf_path)
    if fs_type == FsType.S3:
        if not fs_conf.s3.verify:
            disable_warning_log()
        return S3FsAccessor(fs_conf.s3)
    else:
        return LocalFsAccessor()
