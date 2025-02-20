import boto3
import urllib3
from mypy_boto3_s3 import S3Client

from ..common.fs import S3Config


def create_client(conf: S3Config) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=conf.endpoint_url,
        aws_access_key_id=conf.access_key,
        aws_secret_access_key=conf.secret_key,
        verify=conf.verify,
    )


def disable_warning_log():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
