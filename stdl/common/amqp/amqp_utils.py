from .amqp import AmqpHelperBlocking, AmqpHelperMock
from ..env import Env


def create_amqp(env: Env):
    if env.env == "prod":
        return AmqpHelperBlocking(env.amqp)
    else:
        return AmqpHelperMock()
