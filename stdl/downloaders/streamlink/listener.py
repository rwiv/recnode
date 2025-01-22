import json
from dataclasses import dataclass
from enum import Enum

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import Amqp
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.types import IRecorder, RecordState
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
        try:
            content = json.loads(body.decode("utf-8"))
            message = ExitMessage(ExitCommand(content["cmd"]), PlatformType(content["platform"]), content["uid"])
            if message.uid != self.recorder.get_name():
                return
            ch.basic_ack(method.delivery_tag)

            if self.recorder.get_state() == RecordState.WAIT:
                log.info("Still waiting for the Stream")
                return

            if message.cmd == ExitCommand.CANCEL:
                self.recorder.cancel()
            elif message.cmd == ExitCommand.FINISH:
                self.recorder.finish()
            self.close()
        except Exception as e:
            log.error(e)

    def consume(self):
        self.amqp.connect()
        self.amqp.assert_queue(queue_name)
        self.amqp.consume(queue_name, self.on_message)

    def close(self):
        self.amqp.close()
