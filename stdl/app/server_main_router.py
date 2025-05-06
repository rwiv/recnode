import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, constr

from ..common.spec import PlatformType
from ..data.live import LiveStateService
from ..recorder import RecordingScheduler


class CancelRequest(BaseModel):
    platform: PlatformType
    uid: constr(min_length=1)


class MainController:
    def __init__(self, scheduler: RecordingScheduler, live_state_service: LiveStateService):
        self.__scheduler = scheduler
        self.__live_state_service = live_state_service

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/my-ip", self.my_ip, methods=["GET"])
        self.router.add_api_route("/recordings/{record_id}", self.record, methods=["POST"])
        self.router.add_api_route("/recordings/{record_id}", self.cancel, methods=["DELETE"])
        self.router.add_api_route("/recordings", self.get_status, methods=["GET"])

    def health(self):
        return {"status": "UP"}

    def my_ip(self):
        return requests.get("https://api.ipify.org").text

    def record(self, record_id: str):
        state = self.__live_state_service.get(record_id)
        if state is None:
            raise HTTPException(status_code=404, detail="live stste not found")
        self.__scheduler.record(state)
        return "ok"

    def cancel(self, record_id: str):
        state = self.__live_state_service.get(record_id)
        if state is None:
            raise HTTPException(status_code=404, detail="live state not found")
        self.__scheduler.cancel(state)
        return "ok"

    def get_status(self, fields: str | None = None):
        field_elems = fields.split(",") if fields else []
        with_stats = False
        if "stats" in field_elems:
            with_stats = True

        full_stats = False
        if "full_stats" in field_elems:
            with_stats = True
            full_stats = True
        return self.__scheduler.ger_status(with_stats=with_stats, full_stats=full_stats)
