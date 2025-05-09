import logging

import uvicorn
from fastapi import FastAPI, Request, Response
from pyutils import log, stacktrace
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .server_main_router import MainController
from ..config import get_env
from ..data.live import LiveStateService
from ..data.redis import RedisMap, create_redis_pool
from ..metric import MetricManager
from ..recorder import RecordingScheduler, disable_streamlink_log


async def handle_error(request: Request, call_next):
    try:
        response: Response = await call_next(request)
        return response
    except Exception as ex:
        response = JSONResponse(
            status_code=500,
            content={
                "message": str(ex),
                "method": request.method,
                "url": str(request.url),
                "stacktrace": stacktrace(),
            },
        )
        return response


def run_server():
    log.set_level(logging.DEBUG)
    disable_streamlink_log()

    env = get_env()
    metric = MetricManager()
    scheduler = RecordingScheduler(env, metric)

    redis_pool = create_redis_pool(env.redis)
    redis_client = Redis(connection_pool=redis_pool)
    redis_map = RedisMap(redis_client)

    live_state_service = LiveStateService(redis_map)
    main_controller = MainController(scheduler, live_state_service)

    app = FastAPI()
    app.add_middleware(BaseHTTPMiddleware, dispatch=handle_error)
    app.include_router(main_controller.router)

    uvicorn.run(app, port=env.port, host="0.0.0.0", access_log=False)
