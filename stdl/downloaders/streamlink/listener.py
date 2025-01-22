import json
from dataclasses import dataclass
from enum import Enum

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.interface import IRecorder
from stdl.utils.logger import log


queue_name = "stdl:exit"


class ExitCommand(Enum):
    DELETE = "delete"
    CANCEL = "cancel"
    FINISH = "finish"


@dataclass
class ExitMessage:
    cmd: ExitCommand
    platform: PlatformType
    uid: str


class Listener:

    def __init__(self, recorder: IRecorder, amqp: Amqp):
        self.recorder = recorder
        self.amqp = amqp

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        content = json.loads(body.decode("utf-8"))
        message = ExitMessage(ExitCommand(content["cmd"]), PlatformType(content["platform"]), content["uid"])
        if message.uid != self.recorder.get_name():
            return
        print(message)

        if message.cmd == ExitCommand.CANCEL:
            log.info("Receive Cancel Command")
            self.recorder.cancel()
        elif message.cmd == ExitCommand.FINISH:
            log.info("Receive Finish Command")
            self.recorder.finish()
        ch.basic_ack(method.delivery_tag)
        self.close()

    def consume(self):
        self.amqp.connect()
        self.amqp.assert_queue(queue_name)
        self.amqp.consume(queue_name, self.on_message)

    def close(self):
        self.amqp.close()
