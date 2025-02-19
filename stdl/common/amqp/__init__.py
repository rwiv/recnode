import os
import sys

from .amqp import AmqpHelper, AmqpHelperBlocking, AmqpHelperMock
from .amqp_utils import create_amqp

targets = ["amqp", "amqp_utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
