import logging

import uvicorn
from fastapi import FastAPI
from pyutils import log

from .proxy_main_router import ProxyMainController
from ..common.env import get_env
from ..common.fs import create_fs_writer


def run_proxy():
    log.set_level(logging.DEBUG)

    env = get_env()
    writer = create_fs_writer(env)
    main_controller = ProxyMainController(writer)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=9083, host="0.0.0.0")
