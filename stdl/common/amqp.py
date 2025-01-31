from abc import abstractmethod
from typing import Callable, Optional

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.env import AmqpConfig
from stdl.utils.logger import log


class Amqp:
    @abstractmethod
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        pass

    @abstractmethod
    def create_connection(self) -> BlockingConnection:
        pass

    @abstractmethod
    def create_channel(self) -> BlockingChannel:
        pass

    @abstractmethod
    def assert_queue(self, queue_name: str, auto_delete: bool = False):
        pass

    @abstractmethod
    def consume(self, queue_name: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
        pass

    @abstractmethod
    def publish(self, queue_name: str, body: bytes):
        pass

    @abstractmethod
    def close(self):
        pass


class AmqpBlocking(Amqp):
    def __init__(self, conf: AmqpConfig):
        self.url = f"amqp://{conf.username}:{conf.password}@{conf.host}:{conf.port}"
        self.conn: Optional[BlockingConnection] = None
        self.ch: Optional[BlockingChannel] = None

    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        if self.conn is None or self.conn.is_closed:
            self.conn = self.create_connection()
            log.debug("AMQP connection created")
        if self.ch is None or self.ch.is_closed:
            self.ch = self.create_channel()
            log.debug("AMQP channel created")
        return self.conn, self.ch

    def create_connection(self) -> BlockingConnection:
        return BlockingConnection(pika.URLParameters(self.url))

    def create_channel(self) -> BlockingChannel:
        if not self.conn:
            raise ValueError("Connection not created")
        return self.conn.channel()

    def assert_queue(self, queue_name: str, auto_delete: bool = False):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.queue_declare(
            queue=queue_name, auto_delete=auto_delete,
            passive=False, durable=False, exclusive=False,
        )

    def consume(self, queue_name: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.basic_consume(queue=queue_name, on_message_callback=callback)
        self.ch.start_consuming()

    def publish(self, queue_name: str, body: bytes):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.basic_publish(exchange="", routing_key=queue_name, body=body)

    def close(self):
        try:
            if self.conn is not None and self.conn.is_open:
                self.conn.close()
                log.debug("AMQP connection closed")
            if self.ch is not None and self.ch.is_open:
                self.ch.close()
                log.debug("AMQP channel closed")
            self.conn = None
            self.ch = None
        except Exception as e:
            log.error("Error closing AMQP connection")
            log.error(e)


class AmqpMock(Amqp):
    def connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        log.info("AmqpMock.connect()")
        return None, None

    def create_connection(self) -> BlockingConnection:
        log.info("AmqpMock.create_connection()")
        return None

    def create_channel(self) -> BlockingChannel:
        log.info("AmqpMock.create_channel()")
        return None

    def assert_queue(self, queue_name: str, auto_delete: bool = False):
        log.info(f"AmqpMock.assert_queue({queue_name}, {auto_delete})")
        pass

    def consume(self, queue_name: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
        log.info(f"AmqpMock.consume({queue_name})")
        pass

    def publish(self, queue_name: str, body: bytes):
        log.info(f"AmqpMock.publish({queue_name}, {body})")
        pass

    def close(self):
        log.info("AmqpMock.close()")
        pass
