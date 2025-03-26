import json

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties
from pyutils import log, error_dict

from ..spec.exit_message import ExitMessage, ExitCommand
from ..spec.recording_constants import EXIT_QUEUE_PREFIX
from ..spec.recording_schema import RecordingState, RecordingInfo
from ...common.amqp import AmqpHelper


class RecorderListener:

    def __init__(self, info: RecordingInfo, state: RecordingState, amqp: AmqpHelper, max_retry: int = 10):
        self.info = info
        self.state = state
        self.amqp = amqp
        self.conn: BlockingConnection | None = None
        self.max_retry = max_retry

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            message = ExitMessage(**json.loads(body.decode("utf-8")))
            if message.uid != self.info.uid:
                return
            ch.basic_ack(method.delivery_tag)
            log.info("Received Exit Message")

            if message.cmd == ExitCommand.CANCEL:
                self.state.cancel()
            elif message.cmd == ExitCommand.FINISH:
                self.state.finish()

            ch.stop_consuming()
        except Exception as e:
            log.error("Failed to handle message", error_dict(e))

    def consume(self):
        for retry_cnt in range(self.max_retry + 1):
            try:
                platform = self.info.platform.value
                vid_queue_name = f"{EXIT_QUEUE_PREFIX}.{platform}.{self.info.uid}"
                self.conn, chan = self.amqp.connect()
                self.amqp.ensure_queue(chan, vid_queue_name, auto_delete=True)
                self.amqp.consume(chan, vid_queue_name, self.on_message)
                break
            except Exception as e:
                err_info = error_dict(e)
                err_info["retry_cnt"] = retry_cnt
                log.error("Failed to __consume", err_info)
                if retry_cnt == self.max_retry:
                    self.state.finish()
                    raise
            finally:
                if self.conn is not None:
                    self.amqp.close(self.conn)
                self.conn = None
