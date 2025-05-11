import pytest
from pyutils import path_join, find_project_root

from stdl.file import read_fs_config_by_file, FsConfig, create_async_client

fs_name = "minio"
fs_configs = read_fs_config_by_file(path_join(find_project_root(), "dev", "fs_conf_test.yaml"))
fs_conf: FsConfig | None = None
for conf in fs_configs:
    if conf.name == fs_name:
        fs_conf = conf
        break

if fs_conf is None or fs_conf.s3 is None:
    raise ValueError("Cannot find S3 configuration")
s3_conf = fs_conf.s3


@pytest.mark.asyncio
async def test_s3():
    async with create_async_client(s3_conf) as client:
        data = b"Hello, World!"
        key = "test.txt"
        resp = await client.put_object(Bucket=s3_conf.bucket_name, Key=key, Body=data)
        status = resp["ResponseMetadata"]["HTTPStatusCode"]
        print(status)

        resp = await client.get_object_acl(Bucket=s3_conf.bucket_name, Key=key)
        print(resp["ResponseMetadata"]["HTTPStatusCode"])
