import os
from pathlib import Path

from pyutils import path_join, log

from ..common.env import get_env
from ..common.fs import create_fs_writer, FsType
from ..common.watcher import ChunkWatcher, FsChunkHandler, MockChunkHandler


class WatcherRunner:
    def __init__(self):
        self.env = get_env()
        if self.env.fs_name == FsType.LOCAL:
            raise ValueError("WatcherRunner not supported for local fs")
        self.writer = create_fs_writer(env=self.env, is_watcher=True)

    def run(self):
        target_path = path_join(self.env.out_dir_path, "incomplete")
        if not Path(target_path).exists():
            os.makedirs(target_path, exist_ok=True)
        log.info(f"Start watching: {target_path}")
        watcher = ChunkWatcher(self.env, self.__create_handler(), target_path)
        watcher.watch()

    def __create_handler(self):
        if self.env.env != "prod":
            return MockChunkHandler()
        else:
            return FsChunkHandler(self.writer)
