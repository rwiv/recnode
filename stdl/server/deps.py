from stdl.common.env import get_env
from stdl.server.main_router import MainController
from stdl.app.recording_scheduler import RecordingScheduler


class ServerDependencyManager:
    def __init__(self):
        self.env = get_env()
        self.scheduler = RecordingScheduler(self.env)
        self.__main_controller = MainController(self.scheduler)
        self.main_router = self.__main_controller.router


deps = ServerDependencyManager()
