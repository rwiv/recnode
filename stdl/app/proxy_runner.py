import logging

import uvicorn
from fastapi import FastAPI
from pyutils import log

from .proxy_main_router import ProxyMainController
from ..config import get_proxy_env
from ..file import read_fs_config_by_file


def run_proxy():
    log.set_level(logging.DEBUG)

    env = get_proxy_env()

    fs_configs = read_fs_config_by_file(env.fs_config_path)
    main_controller = ProxyMainController(fs_configs)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=env.port, host="0.0.0.0", access_log=False)
