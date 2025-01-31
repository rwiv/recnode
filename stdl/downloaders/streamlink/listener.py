import json

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import Amqp
from stdl.downloaders.streamlink.types import AbstractRecorder, RecordState
from stdl.event.exit_message import ExitMessage, ExitCommand
from stdl.utils.logger import log


# TODO: change `:` to `.`
EXIT_QUEUE_PREFIX = "stdl:exit"


class RecorderListener:

    def __init__(self, recorder: AbstractRecorder, amqp: Amqp):
        self.recorder = recorder
        self.amqp = amqp

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            message = ExitMessage(**json.loads(body.decode("utf-8")))
            if message.uid != self.recorder.uid:
                return
            ch.basic_ack(method.delivery_tag)

            # if self.recorder.get_state() == RecordState.WAIT:
            #     log.info("Still waiting for the Stream")
            #     return

            if message.cmd == ExitCommand.CANCEL:
                self.recorder.cancel()
            elif message.cmd == ExitCommand.FINISH:
                self.recorder.finish()

            self.close()
        except Exception as e:
            log.error("Failed to handle message")
            log.error(e)

    def consume(self):
        try:
            platform = self.recorder.platform_type.value
            # TODO: change `:` to `.`
            vid_queue_name = f"{EXIT_QUEUE_PREFIX}:{platform}:{self.recorder.uid}"
            self.amqp.connect()
            self.amqp.assert_queue(vid_queue_name, auto_delete=True)
            self.amqp.consume(vid_queue_name, self.on_message)
        except Exception as e:
            log.error("Failed to consume")
            log.error(e)

    def close(self):
        self.amqp.close()
