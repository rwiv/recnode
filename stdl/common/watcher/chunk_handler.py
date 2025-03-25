from abc import ABC, abstractmethod


class ChunkHandler(ABC):
    @abstractmethod
    def handle(self, file_path: str):
        pass
