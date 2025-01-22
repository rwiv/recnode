from abc import abstractmethod


class IRecorder:
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def finish(self):
        pass
