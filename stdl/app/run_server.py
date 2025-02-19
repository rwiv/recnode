import uvicorn
from fastapi import FastAPI

from .main_router import MainController
from ..common.env import get_env
from ..record import RecordingScheduler, disable_streamlink_log


def run_server():
    disable_streamlink_log()

    env = get_env()
    scheduler = RecordingScheduler(env)
    main_controller = MainController(scheduler)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=9083, host="0.0.0.0")
