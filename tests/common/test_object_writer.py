import pytest
from pyutils import path_join, find_project_root

from recnode.file import read_fs_config_by_file, FsConfig
from recnode.file.object_writer import S3ObjectWriter

fs_name = "minio"
fs_configs = read_fs_config_by_file(path_join(find_project_root(), "dev", "fs_conf_test.yaml"))
fs_conf: FsConfig | None = None
for conf in fs_configs:
    if conf.name == fs_name:
        fs_conf = conf
        break


@pytest.mark.asyncio
async def test_object_writer():
    if fs_conf is None or fs_conf.s3 is None:
        raise ValueError("Cannot find S3 configuration")
    writer = S3ObjectWriter(fs_name=fs_name, conf=fs_conf.s3)
    await writer.write("test.txt", b"Hello, World!")
