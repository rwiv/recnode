from fastapi import APIRouter

from stdl.server.recording_scheduler import RecordingScheduler


class MainController:
    def __init__(self, scheduler: RecordingScheduler):
        self.scheduler = scheduler

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/record/{uid}", self.record, methods=["GET"])

    def health(self):
        return "ok"

    def record(self, uid: str):
        return uid
