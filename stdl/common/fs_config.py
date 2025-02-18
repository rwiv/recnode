import yaml
from pydantic import BaseModel, Field


class S3Config(BaseModel):
    name: str = Field(min_length=1)
    endpoint_url: str = Field(min_length=1, alias="endpointUrl")
    access_key: str = Field(min_length=1, alias="accessKey")
    secret_key: str = Field(min_length=1, alias="secretKey")
    verify: bool
    bucket: str = Field(min_length=1)


class FsConfig(BaseModel):
    s3: list[S3Config]


def read_fs_config_by_file(config_path: str) -> FsConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return FsConfig(**yaml.load(text, Loader=yaml.FullLoader))
