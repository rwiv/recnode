import yaml
from pydantic import BaseModel
from pynifs import S3Config


class FsConfig(BaseModel):
    s3: S3Config


def read_fs_config_by_file(config_path: str) -> FsConfig:
    with open(config_path, "r") as file:
        text = file.read()
    return FsConfig(**yaml.load(text, Loader=yaml.FullLoader))
