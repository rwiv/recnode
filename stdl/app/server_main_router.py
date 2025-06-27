import aiohttp
from fastapi import APIRouter, HTTPException
from prometheus_client import generate_latest
from pydantic import BaseModel, constr
from starlette.responses import Response

from ..common import PlatformType
from ..data.live import LiveStateService
from ..recorder import RecordingScheduler
from ..utils import HttpRequestError


class CancelRequest(BaseModel):
    platform: PlatformType
    uid: constr(min_length=1)


class MainController:
    def __init__(
        self,
        scheduler: RecordingScheduler,
        live_service: LiveStateService,
    ):
        self.__scheduler = scheduler
        self.__live_service = live_service

        self.router = APIRouter()
        self.router.add_api_route("/metrics", self.metrics, methods=["GET"])
        self.router.add_api_route("/api/health", self.health, methods=["GET"])
        self.router.add_api_route("/api/my-ip", self.my_ip, methods=["GET"])
        self.router.add_api_route("/api/recordings/{record_id}", self.record, methods=["POST"])
        self.router.add_api_route("/api/recordings/{record_id}", self.cancel, methods=["DELETE"])
        self.router.add_api_route("/api/recordings", self.get_status, methods=["GET"])

    def health(self):
        return {"status": "UP"}

    async def my_ip(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(url="https://api.ipify.org") as res:
                if res.status >= 400:
                    raise HttpRequestError.from_response("Failed to request", res)
                return await res.text()

    def metrics(self):
        return Response(content=generate_latest(), media_type="text/plain")

    async def record(self, record_id: str):
        state = await self.__live_service.get_live(record_id, use_master=True)
        if state is None:
            raise HTTPException(status_code=404, detail="Not found LiveState")

        for summary in self.__scheduler.get_recorder_summaries():
            if (
                summary.platform == state.platform
                and summary.channel_id == state.channel_id
                and summary.video_name == state.video_name
            ):
                raise HTTPException(status_code=422, detail="Already recording live")

        self.__scheduler.record(state)
        return "ok"

    async def cancel(self, record_id: str):
        state = await self.__live_service.get_live(record_id, use_master=True)
        if state is None:
            raise HTTPException(status_code=404, detail="live state not found")
        self.__scheduler.cancel(state)
        return "ok"

    async def get_status(self, fields: str | None = None):
        field_elems = fields.split(",") if fields else []

        with_stats = False
        if "stats" in field_elems:
            with_stats = True

        full_stats = False
        if "full_stats" in field_elems:
            with_stats = True
            full_stats = True

        with_resources = False
        if "resources" in field_elems:
            with_resources = True

        return await self.__scheduler.get_status(
            with_stats=with_stats,
            full_stats=full_stats,
            with_resources=with_resources,
        )
