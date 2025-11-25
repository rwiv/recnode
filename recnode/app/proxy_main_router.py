import aiohttp
from fastapi import APIRouter, UploadFile, File

from ..file import FsConfig, create_proxy_fs_writer
from ..utils import HttpRequestError


class ProxyMainController:
    def __init__(self, configs: list[FsConfig]):
        self.__configs = configs

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

    async def upload(self, file: UploadFile = File(...), path: str = File(...), fs_name: str = File(...)):
        writer = create_proxy_fs_writer(fs_name=fs_name, configs=self.__configs)
        await writer.write(path, await file.read())
