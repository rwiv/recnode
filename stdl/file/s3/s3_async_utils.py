from aiobotocore.session import get_session
from types_aiobotocore_s3.client import S3Client

from ..fs.fs_config import S3Config


def create_async_client(conf: S3Config) -> S3Client:
    client = get_session().create_client(
        "s3",
        endpoint_url=conf.endpoint_url,
        aws_access_key_id=conf.access_key,
        aws_secret_access_key=conf.secret_key,
        verify=conf.verify,
    )
    return client  # type: ignore
