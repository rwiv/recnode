import logging

import uvicorn
from fastapi import FastAPI
from pyutils import log

from .server_main_router import MainController
from ..common.env import get_env
from ..data.live import LiveStateService
from ..data.redis import create_redis_client, RedisMap
from ..recorder import RecordingScheduler, disable_streamlink_log


def run_server():
    log.set_level(logging.DEBUG)
    disable_streamlink_log()

    env = get_env()
    scheduler = RecordingScheduler(env)

    redis_client = create_redis_client(env.redis)
    redis_map = RedisMap(redis_client)

    live_state_service = LiveStateService(redis_map)
    main_controller = MainController(scheduler, live_state_service)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=env.port, host="0.0.0.0", access_log=False)
