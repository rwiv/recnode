from stdl.common.amqp import AmqpHelperBlocking, AmqpHelperMock
from stdl.common.env import Env


def create_amqp(env: Env):
    # return AmqpHelperBlocking(self.env.amqp)
    if env.env == "prod":
        return AmqpHelperBlocking(env.amqp)
    else:
        return AmqpHelperMock()
