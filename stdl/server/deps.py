from stdl.common.amqp import AmqpHelperBlocking, AmqpHelperMock
from stdl.common.env import get_env
from stdl.server.main_router import MainController
from stdl.server.recording_scheduler import RecordingScheduler


class ServerDependencyManager:
    def __init__(self):
        self.env = get_env()
        self.scheduler = RecordingScheduler(self.env)
        self.__main_controller = MainController(self.scheduler)
        self.main_router = self.__main_controller.router

    def create_amqp(self):
        if self.env.env == "prod":
            return AmqpHelperBlocking(self.env.amqp)
        else:
            return AmqpHelperMock()


deps = ServerDependencyManager()
