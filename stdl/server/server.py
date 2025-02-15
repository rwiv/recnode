import uvicorn
from fastapi import FastAPI

from stdl.server.deps import deps


def run():
    env = deps.env

    app = FastAPI()

    app.include_router(deps.main_router)

    uvicorn.run(app, port=9083, host="0.0.0.0")
