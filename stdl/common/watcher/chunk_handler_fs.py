import os
import time

from pyutils import log, path_join, split_path, error_dict

from .chunk_handler import ChunkHandler
from ..fs import ObjectWriter, FsType


class FsChunkHandler(ChunkHandler):
    def __init__(self, writer: ObjectWriter):
        super().__init__()
        self.writer = writer
        self.write_retry_limit = 2
        self.write_retry_delay_sec = 1

    def handle(self, file_path: str):
        if isinstance(file_path, bytes):
            raise ValueError("event.src_path is not a string")
        if self.writer.fs_type == FsType.LOCAL:
            raise ValueError("FsChunkHandler only supports S3")

        chunks = split_path(file_path)
        file_name = chunks[-1]
        video_name = chunks[-2]
        uid = chunks[-3]

        out_file_path = path_join("incomplete", uid, video_name, file_name)
        self.__write_segment(file_path, out_file_path)
        log.debug(f"Write Segment: {out_file_path}")
        os.remove(file_path)

    def __write_segment(self, tmp_file_path: str, out_file_path: str):
        for retry_cnt in range(self.write_retry_limit + 1):
            try:
                with open(tmp_file_path, "rb") as f:
                    self.writer.write(out_file_path, f.read())
                break
            except Exception as e:
                if retry_cnt == self.write_retry_limit:
                    log.error("Write Segment: Retry Limit Exceeded", error_dict(e))
                    break
                log.error(f"Write Segment: cnt={retry_cnt}", error_dict(e))
                time.sleep(self.write_retry_delay_sec * (2**retry_cnt))
