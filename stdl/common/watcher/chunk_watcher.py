import os
import queue
import threading
import time

from pyutils import stacktrace

from .chunk_handler import ChunkHandler


class ChunkWatcher:
    def __init__(
        self,
        handler: ChunkHandler,
        target_path: str,
        parallel: int = 3,
        threshold_sec: int = 3,
    ):
        self.handler = handler
        self.target_path = target_path
        self.parallel = parallel
        self.threshold_sec = threshold_sec
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
                time.sleep(1)
            except:
                print(stacktrace())
                break

    def __process(self):
        threads = []
        for _ in range(self.parallel):
            if self.queue.empty():
                break
            target = self.queue.get()
            self.set.remove(target)
            thread = threading.Thread(target=self.handler.handle, args=(target,))
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
                mtime = os.stat(file_path).st_mtime
                if current_time - mtime > self.threshold_sec:
                    targets.append((file_path, mtime))

        targets.sort(key=lambda x: x[1])

        return [target[0] for target in targets]
