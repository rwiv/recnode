from fastapi import APIRouter

from stdl.server.recording_scheduler import RecordingScheduler


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

    def record(self, uid: str):
        self.scheduler.record(uid)
        return uid

    def cancel(self, uid: str):
        self.scheduler.cancel(uid)
        return uid

    def get_recording_count(self):
        return self.scheduler.get_recording_count()
