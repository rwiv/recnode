import os
from os.path import join

from pydantic import BaseModel, Field

from stdl.utils.env import load_env
from stdl.utils.path import find_project_root


class AmqpConfig(BaseModel):
    host: str = Field(min_length=1)
    port: int
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class Env(BaseModel):
    env: str
    out_dir_path: str
    tmp_dir_path: str
    config_path: str | None
    amqp: AmqpConfig


def get_env() -> Env:
    env = os.getenv("PY_ENV")
    if env is None:
        env = "dev"
    if env == "dev":
        load_env(join(find_project_root(), "dev", ".env"))

    out_dir_path = os.getenv("OUT_DIR_PATH")
    if out_dir_path is None:
        raise ValueError("OUT_DIR_PATH is not set")
    tmp_dir_path = os.getenv("TMP_DIR_PATH")
    if tmp_dir_path is None:
        raise ValueError("TMP_DIR_PATH is not set")
    config_path = os.getenv("CONFIG_PATH")

    amqp_host = os.getenv("AMQP_HOST")
    amqp_port_str = os.getenv("AMQP_PORT")
    amqp_username = os.getenv("AMQP_USERNAME")
    amqp_password = os.getenv("AMQP_PASSWORD")
    if amqp_host is None or amqp_port_str is None or amqp_username is None or amqp_password is None:
        raise ValueError("AMQP config is not set")
    amqp_config = AmqpConfig(
        host=amqp_host,
        port=int(amqp_port_str),
        username=amqp_username,
        password=amqp_password,
    )

    return Env(
        env=env,
        out_dir_path=out_dir_path,
        tmp_dir_path=tmp_dir_path,
        config_path=config_path,
        amqp=amqp_config,
    )
