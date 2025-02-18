import os

from pydantic import BaseModel

from stdl.utils.env import load_env


class AmqpConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str


class Env(BaseModel):
    env: str
    out_dir_path: str
    tmp_dir_path: str
    config_path: str | None
    amqp: AmqpConfig


def get_env() -> Env:
    env = os.getenv("PY_ENV") or None
    if env is None:
        env = "dev"
    if env == "dev":
        load_env("./dev/.env")

    out_dir_path = os.getenv("OUT_DIR_PATH") or None
    if out_dir_path is None:
        raise ValueError("OUT_DIR_PATH is not set")
    tmp_dir_path = os.getenv("TMP_DIR_PATH") or None
    if tmp_dir_path is None:
        raise ValueError("TMP_DIR_PATH is not set")
    config_path = os.getenv("CONFIG_PATH") or None

    amqp_host = os.getenv("AMQP_HOST") or None
    amqp_port_str = os.getenv("AMQP_PORT") or None
    amqp_username = os.getenv("AMQP_USERNAME") or None
    amqp_password = os.getenv("AMQP_PASSWORD") or None
    if amqp_host is None or amqp_port_str is None or amqp_username is None or amqp_password is None:
        raise ValueError("AMQP config is not set")
    amqp_port = int(amqp_port_str)
    amqp_config = AmqpConfig(
        host=amqp_host,
        port=amqp_port,
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
