from fastapi import APIRouter
from pydantic import BaseModel, Field

from stdl.app.recording_scheduler import RecordingScheduler
from stdl.common.request_config import AppConfig
from stdl.common.types import PlatformType


class CancelRequest(BaseModel):
    platform_type: PlatformType = Field(alias="platformType")
    uid: str


class MainController:
    def __init__(self, scheduler: RecordingScheduler):
        self.scheduler = scheduler

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/recordings/record/{uid}", self.record, methods=["GET"])
        self.router.add_api_route("/recordings/cancel/{uid}", self.cancel, methods=["GET"])
        self.router.add_api_route("/recordings/counts", self.get_recording_count, methods=["GET"])

    def health(self):
        return "ok"

    def record(self, req: AppConfig):
        self.scheduler.record(req)
        return "ok"

    def cancel(self, req: CancelRequest):
        self.scheduler.cancel(req.platform_type, req.uid)
        return "ok"

    def get_recording_count(self):
        return self.scheduler.get_recording_count()
