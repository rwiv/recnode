import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import uvicorn
from fastapi import FastAPI
from pyutils import log

from .proxy_main_router import ProxyMainController
from ..config import get_proxy_env
from ..file import create_proxy_fs_writer
from ..metric import MetricManager


def run_proxy():
    log.set_level(logging.DEBUG)

    env = get_proxy_env()

    metric = MetricManager()
    writer = create_proxy_fs_writer(env, metric)
    main_controller = ProxyMainController(writer)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=env.port, host="0.0.0.0", access_log=False)
