import aiohttp
from fastapi import APIRouter, UploadFile, File

from ..file import ObjectWriter, FsType
from ..utils import HttpRequestError


class ProxyMainController:
    def __init__(self, writer: ObjectWriter):
        self.__write_retry_limit = 8
        self.__write_retry_delay_sec = 0.5

        self.__writer = writer
        if self.__writer.fs_type == FsType.LOCAL:
            raise ValueError("local fs_type is not supported")

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/my-ip", self.my_ip, methods=["GET"])
        self.router.add_api_route("/upload", self.upload, methods=["POST"])

    def health(self):
        return {"status": "UP"}

    async def my_ip(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(url="https://api.ipify.org") as res:
                if res.status >= 400:
                    raise HttpRequestError.from_response("Failed to request", res)
                return await res.text()

    async def upload(self, file: UploadFile = File(...), path: str = File(...)):
        await self.__writer.write(path, await file.read())
