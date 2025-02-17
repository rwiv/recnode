from abc import abstractmethod, ABC
from typing import Callable

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.env import AmqpConfig
from stdl.utils.error import stacktrace
from stdl.utils.logger import log


class Amqp(ABC):
    @abstractmethod
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        pass

    @abstractmethod
    def assert_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
        pass

    @abstractmethod
    def consume(
        self,
        chan: BlockingChannel,
        queue_name: str,
        callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None],
    ):
        pass

    @abstractmethod
    def publish(self, chan: BlockingChannel, queue_name: str, body: bytes):
        pass

    @abstractmethod
    def close(self, conn: BlockingConnection):
        pass


class AmqpBlocking(Amqp):
    def __init__(self, conf: AmqpConfig):
        self.url = f"amqp://{conf.username}:{conf.password}@{conf.host}:{conf.port}"

    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        conn = BlockingConnection(pika.URLParameters(self.url))
        chan = conn.channel()
        return conn, chan

    def assert_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
        chan.queue_declare(
            queue=queue_name,
            auto_delete=auto_delete,
            passive=False,
            durable=False,
            exclusive=False,
        )

    def consume(
        self,
        chan: BlockingChannel,
        queue_name: str,
        callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None],
    ):
        chan.basic_consume(queue=queue_name, on_message_callback=callback)
        chan.start_consuming()

    def publish(self, chan: BlockingChannel, queue_name: str, body: bytes):
        chan.basic_publish(exchange="", routing_key=queue_name, body=body)

    def close(self, conn: BlockingConnection):
        try:
            if not conn.is_closed:
                conn.close()
                log.debug("AMQP connection closed")
        except:
            log.error("Error closing AMQP connection")
            print(stacktrace())


class AmqpMock(Amqp):
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        log.info("AmqpMock.connect()")
        return None, None  # type: ignore

    def assert_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
        log.info(f"AmqpMock.assert_queue({queue_name}, {auto_delete})")
        pass

    def consume(
        self,
        chan: BlockingChannel,
        queue_name: str,
        callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None],
    ):
        log.info(f"AmqpMock.consume({queue_name})")
        pass

    def publish(self, chan: BlockingChannel, queue_name: str, body: bytes):
        log.info(f"AmqpMock.publish({queue_name}, {body})")
        pass

    def close(self, conn: BlockingConnection):
        log.info("AmqpMock.close()")
        pass
