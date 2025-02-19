from fastapi import APIRouter
from pydantic import BaseModel, Field

from ..common import PlatformType, AppConfig
from ..record import RecordingScheduler


class CancelRequest(BaseModel):
    platform_type: PlatformType = Field(alias="platformType")
    uid: str


class MainController:
    def __init__(self, scheduler: RecordingScheduler):
        self.scheduler = scheduler

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/recordings", self.record, methods=["POST"])
        self.router.add_api_route("/recordings", self.cancel, methods=["DELETE"])
        self.router.add_api_route("/recordings", self.get_status, methods=["GET"])

    def health(self):
        return "ok"

    def record(self, req: AppConfig):
        self.scheduler.record(req)
        return "ok"

    def cancel(self, req: CancelRequest):
        self.scheduler.cancel(req.platform_type, req.uid)
        return "ok"

    def get_status(self):
        return self.scheduler.ger_status()
