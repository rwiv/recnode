import os

from pydantic import BaseModel, constr, conint


class AmqpConfig(BaseModel):
    host: constr(min_length=1)
    port: conint(ge=1)
    username: constr(min_length=1)
    password: constr(min_length=1)


def read_amqp_config() -> AmqpConfig:
    return AmqpConfig(
        host=os.getenv("AMQP_HOST"),
        port=os.getenv("AMQP_PORT"),
        username=os.getenv("AMQP_USERNAME"),
        password=os.getenv("AMQP_PASSWORD"),
    )
