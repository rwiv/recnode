import uvicorn
from fastapi import FastAPI

from stdl.common.env import get_env
from stdl.record.recording_scheduler import RecordingScheduler
from stdl.record.utils.streamlink_utils import disable_streamlink_log
from stdl.app.main_router import MainController


def run():
    disable_streamlink_log()

    env = get_env()
    scheduler = RecordingScheduler(env)
    main_controller = MainController(scheduler)

    app = FastAPI()
    app.include_router(main_controller.router)

    uvicorn.run(app, port=9083, host="0.0.0.0")
