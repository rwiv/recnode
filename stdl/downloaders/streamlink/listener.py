import json

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import AmqpHelper
from stdl.downloaders.streamlink.types import AbstractRecorder
from stdl.event.exit_message import ExitMessage, ExitCommand
from stdl.utils.error import stacktrace
from stdl.utils.logger import log


EXIT_QUEUE_PREFIX = "stdl.exit"


class RecorderListener:

    def __init__(self, recorder: AbstractRecorder, amqp: AmqpHelper):
        self.recorder = recorder
        self.amqp = amqp
        self.conn: BlockingConnection | None = None

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            message = ExitMessage(**json.loads(body.decode("utf-8")))
            if message.uid != self.recorder.uid:
                return
            ch.basic_ack(method.delivery_tag)
            log.info("Received Exit Message")

            if message.cmd == ExitCommand.CANCEL:
                self.recorder.cancel()
            elif message.cmd == ExitCommand.FINISH:
                self.recorder.finish()

            ch.stop_consuming()
        except:
            log.error("Failed to handle message")
            print(stacktrace())

    def consume(self):
        try:
            platform = self.recorder.platform_type.value
            vid_queue_name = f"{EXIT_QUEUE_PREFIX}.{platform}.{self.recorder.uid}"
            self.conn, chan = self.amqp.connect()
            self.amqp.ensure_queue(chan, vid_queue_name, auto_delete=True)
            self.amqp.consume(chan, vid_queue_name, self.on_message)
        except:
            log.error("Failed to __consume")
            print(stacktrace())
        finally:
            if self.conn is not None:
                self.amqp.close(self.conn)
            self.conn = None
