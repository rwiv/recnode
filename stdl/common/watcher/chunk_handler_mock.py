import os
import random
import time

from pyutils import log

from .chunk_handler import ChunkHandler


class MockChunkHandler(ChunkHandler):
    def handle(self, file_path: str):
        log.info(f"Handling {file_path}")
        time.sleep(random.uniform(5, 10))
        os.remove(file_path)
        log.info(f"Removed {file_path}")
