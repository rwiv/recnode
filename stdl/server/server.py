import uvicorn
from fastapi import FastAPI

from stdl.server.deps import deps
from stdl.utils.streamlink import disable_streamlink_log


def run():
    disable_streamlink_log()

    app = FastAPI()
    app.include_router(deps.main_router)

    uvicorn.run(app, port=9083, host="0.0.0.0")
