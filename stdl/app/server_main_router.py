from fastapi import APIRouter
from pydantic import BaseModel, constr

from ..common.request import AppRequest
from ..common.spec import PlatformType
from ..recorder import RecordingScheduler


class CancelRequest(BaseModel):
    platform: PlatformType
    uid: constr(min_length=1)


class MainController:
    def __init__(self, scheduler: RecordingScheduler):
        self.scheduler = scheduler

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/recordings", self.record, methods=["POST"])
        self.router.add_api_route("/recordings", self.cancel, methods=["DELETE"])
        self.router.add_api_route("/recordings", self.get_status, methods=["GET"])

    def health(self):
        return {"status": "UP"}

    def record(self, req: AppRequest):
        self.scheduler.record(req)
        return "ok"

    def cancel(self, req: CancelRequest):
        self.scheduler.cancel(req.platform, req.uid)
        return "ok"

    def get_status(self):
        return self.scheduler.ger_status()
