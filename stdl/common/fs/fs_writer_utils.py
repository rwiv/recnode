from pathlib import Path

from .fs_config import read_fs_config_by_file
from .fs_types import FsType
from .fs_writer import FsWriter, LocalFsWriter, S3FsWriter
from ...utils.s3_utils import disable_warning_log


def create_fs_writer(fs_type: FsType, fs_conf_path: str | None) -> FsWriter:
    if fs_type == FsType.LOCAL or fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalFsWriter()
    fs_conf = read_fs_config_by_file(fs_conf_path)
    if fs_type == FsType.S3:
        if not fs_conf.s3.verify:
            disable_warning_log()
        return S3FsWriter(fs_conf.s3)
    else:
        return LocalFsWriter()
