from abc import abstractmethod, ABC
from typing import Callable

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.exceptions import ChannelClosedByBroker
from pika.spec import Basic, BasicProperties
from pyutils import log, error_dict

from ..env import AmqpConfig


class AmqpHelper(ABC):
    @abstractmethod
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        pass

    @abstractmethod
    def queue_exists(self, chan: BlockingChannel, queue_name: str) -> bool:
        pass

    @abstractmethod
    def ensure_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
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


class AmqpHelperBlocking(AmqpHelper):
    def __init__(self, conf: AmqpConfig):
        self.conf = conf
        self.__amqp_url = f"amqp://{conf.username}:{conf.password}@{conf.host}:{conf.port}"

    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        conn = BlockingConnection(pika.URLParameters(self.__amqp_url))
        chan = conn.channel()
        return conn, chan

    def queue_exists(self, chan: BlockingChannel, queue_name: str) -> bool:
        try:
            chan.queue_declare(queue=queue_name, passive=True)
            return True
        except ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
            else:
                raise e

    def ensure_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
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
        except Exception as e:
            log.error("Error closing AMQP connection", error_dict(e))


class AmqpHelperMock(AmqpHelper):
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        log.info("AmqpMock.connect()")
        return None, None  # type: ignore

    def queue_exists(self, chan: BlockingChannel, queue_name: str) -> bool:
        return False

    def ensure_queue(self, chan: BlockingChannel, queue_name: str, auto_delete: bool = False):
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
