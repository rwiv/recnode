import json

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.types import IRecorder, RecordState
from stdl.event.exit_message import ExitMessage, ExitCommand
from stdl.utils.logger import log


# TODO: change `:` to `.`
queue_prefix = "stdl:exit"


class RecorderListener:

    def __init__(self, recorder: IRecorder, amqp: Amqp):
        self.recorder = recorder
        self.amqp = amqp

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            message = ExitMessage(**json.loads(body.decode("utf-8")))
            if message.uid != self.recorder.get_uid():
                return
            ch.basic_ack(method.delivery_tag)

            if self.recorder.get_state() == RecordState.WAIT:
                log.info("Still waiting for the Stream")
                return

            if message.cmd == ExitCommand.CANCEL:
                self.recorder.cancel()

            self.close()
        except Exception as e:
            log.error(e)

    def consume(self):
        platform = self.recorder.get_platform_type().value
        # TODO: change `:` to `.`
        queue_name = f"{queue_prefix}:{platform}:{self.recorder.get_uid()}"
        self.amqp.connect()
        self.amqp.assert_queue(queue_name, auto_delete=True)
        self.amqp.consume(queue_name, self.on_message)

    def close(self):
        self.amqp.close()
