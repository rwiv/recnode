import os
import queue
import threading
import time

from pyutils import stacktrace, log, error_dict

from .chunk_handler import ChunkHandler
from ..env import Env


class ChunkWatcher:
    def __init__(
        self,
        env: Env,
        handler: ChunkHandler,
        target_path: str,
    ):
        self.handler = handler
        self.target_path = target_path
        conf = env.watcher
        if conf is None:
            raise ValueError("Watcher config is not set")
        self.parallel = conf.parallel
        self.threshold_sec = conf.threshold_sec
        self.queue = queue.Queue()
        self.set = set()

    def watch(self):
        while True:
            try:
                for file_path in self.read_dir_recur(self.target_path):
                    if file_path not in self.set:
                        self.queue.put(file_path)
                        self.set.add(file_path)
                if not self.queue.empty():
                    self.__process()
                time.sleep(0.1)
            except:
                print(stacktrace())
                break
        log.info("Watcher stopped")

    def __process(self):
        threads = []
        for _ in range(self.parallel):
            if self.queue.empty():
                break
            target_path = self.queue.get()
            try:
                self.set.remove(target_path)  # set.discard() is not used to check for errors
            except Exception as e:
                log.error(f"Failed to remove from set: {target_path}", error_dict(e))
            thread = threading.Thread(target=self.handler.handle, args=(target_path,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def read_dir_recur(self, dir_path: str):
        current_time = time.time()
        targets = []
        for root, _, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                if file_path in self.set:
                    continue
                mtime = os.stat(file_path).st_mtime
                if current_time - mtime > self.threshold_sec:
                    targets.append((file_path, mtime))

        targets.sort(key=lambda x: x[1])

        return [target[0] for target in targets]
