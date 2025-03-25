import os
import sys

from .chunk_watcher import ChunkWatcher
from .chunk_handler import ChunkHandler
from .chunk_handler_fs import FsChunkHandler
from .chunk_handler_mock import MockChunkHandler

targets = [
    "chunk_handler_fs",
    "chunk_handler_mock",
    "chunk_watcher",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
