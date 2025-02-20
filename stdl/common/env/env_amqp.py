import os

from pydantic import BaseModel, Field


class AmqpConfig(BaseModel):
    host: str = Field(min_length=1)
    port: int
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


def read_amqp_config() -> AmqpConfig:
    amqp_host = os.getenv("AMQP_HOST")
    amqp_port_str = os.getenv("AMQP_PORT")
    amqp_username = os.getenv("AMQP_USERNAME")
    amqp_password = os.getenv("AMQP_PASSWORD")
    if amqp_host is None or amqp_port_str is None or amqp_username is None or amqp_password is None:
        raise ValueError("AMQP config is not set")
    return AmqpConfig(
        host=amqp_host,
        port=int(amqp_port_str),
        username=amqp_username,
        password=amqp_password,
    )
