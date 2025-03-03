import yaml
from pydantic import BaseModel, Field, constr

from .fs_types import FsType


class S3Config(BaseModel):
    endpoint_url: constr(min_length=1) = Field(alias="endpointUrl")
    access_key: constr(min_length=1) = Field(alias="accessKey")
    secret_key: constr(min_length=1) = Field(alias="secretKey")
    verify: bool
    bucket_name: constr(min_length=1) = Field(alias="bucketName")


class FsConfig(BaseModel):
    name: constr(min_length=1)
    type: FsType
    s3: S3Config | None = None


class FsConfigYaml(BaseModel):
    configs: list[FsConfig]


def read_fs_config_by_file(config_path: str) -> list[FsConfig]:
    with open(config_path, "r") as file:
        text = file.read()
    return FsConfigYaml(**yaml.load(text, Loader=yaml.FullLoader)).configs
