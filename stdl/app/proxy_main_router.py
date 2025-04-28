import time

import requests
from fastapi import APIRouter, UploadFile, File
from pyutils import log, error_dict

from ..file import ObjectWriter, FsType


class ProxyMainController:
    def __init__(self, writer: ObjectWriter):
        self.write_retry_limit = 8
        self.write_retry_delay_sec = 0.5

        self.writer = writer

        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/my-ip", self.my_ip, methods=["GET"])
        self.router.add_api_route("/upload", self.upload, methods=["POST"])

    def health(self):
        return {"status": "UP"}

    def my_ip(self):
        return requests.get("https://api.ipify.org").text

    async def upload(self, file: UploadFile = File(...)):
        data = await file.read()
        file_path = file.filename

        if file_path is None:
            raise ValueError("Filename is None")
        if self.writer.fs_type == FsType.LOCAL:
            raise ValueError("Local file system is not supported")

        self.__write_file(file_path, data)

    def __write_file(self, out_file_path: str, data: bytes):
        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                self.writer.write(out_file_path, data)
                break
            except Exception as e:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", error_dict(e))
                    break
                log.error(f"Write Segment: cnt={retry_cnt}", error_dict(e))
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))
