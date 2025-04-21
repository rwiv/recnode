from fastapi import APIRouter, UploadFile, File

from ..common.fs import ObjectWriter


class ProxyMainController:
    def __init__(self, writer: ObjectWriter):
        self.writer = writer
        self.router = APIRouter(prefix="/api")
        self.router.add_api_route("/health", self.health, methods=["GET"])
        self.router.add_api_route("/upload", self.upload, methods=["POST"])

    def health(self):
        return {"status": "UP"}

    def upload(self, file: UploadFile = File(...)):
        f = file.file
        filename = file.filename
        print(filename)
