import yaml

from pydantic import BaseModel, Field, constr


class S3Config(BaseModel):
    name: constr(min_length=1)
    endpoint_url: constr(min_length=1) = Field(alias="endpointUrl")
    access_key: constr(min_length=1) = Field(alias="accessKey")
    secret_key: constr(min_length=1) = Field(alias="secretKey")
    verify: bool
    bucket_name: constr(min_length=1) = Field(alias="bucketName")


class FsConfig(BaseModel):
    s3: S3Config


def read_fs_config_by_file(config_path: str) -> FsConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return FsConfig(**yaml.load(text, Loader=yaml.FullLoader))
