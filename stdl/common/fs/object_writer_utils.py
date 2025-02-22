from pathlib import Path

from pyutils import find_elem

from .fs_config import read_fs_config_by_file, S3Config
from .fs_types import FsType
from .object_writer import ObjectWriter, LocalObjectWriter, S3ObjectWriter
from ...utils import disable_warning_log


def create_fs_writer(fs_type: FsType, fs_name: str, fs_conf_path: str | None) -> ObjectWriter:
    if fs_type == FsType.LOCAL or fs_conf_path is None or not Path(fs_conf_path).exists():
        return LocalObjectWriter()
    fs_conf = read_fs_config_by_file(fs_conf_path)
    if fs_type == FsType.S3:

        def cond(x: S3Config) -> bool:
            return x.name == fs_name

        s3_conf = find_elem(fs_conf.s3, cond)
        if s3_conf is None:
            raise ValueError(f"Cannot find S3 configuration with name {fs_name}")
        if not s3_conf.verify:
            disable_warning_log()
        return S3ObjectWriter(s3_conf)
    else:
        return LocalObjectWriter()
