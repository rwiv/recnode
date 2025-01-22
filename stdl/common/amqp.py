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
    def assert_queue(self, queue: str):
        pass

    @abstractmethod
    def consume(self, queue: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
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
        self.conn = self.create_connection()
        self.ch = self.create_channel()
        return self.conn, self.ch

    def create_connection(self) -> BlockingConnection:
        return BlockingConnection(pika.URLParameters(self.url))

    def create_channel(self) -> BlockingChannel:
        if not self.conn:
            raise ValueError("Connection not created")
        return self.conn.channel()

    def assert_queue(self, queue: str):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.queue_declare(queue=queue, passive=False, durable=False, auto_delete=False, exclusive=False)

    def consume(self, queue: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.basic_consume(queue=queue, on_message_callback=callback)
        self.ch.start_consuming()

    def close(self):
        try:
            self.ch.close()
            self.conn.close()
            self.ch = None
            self.conn = None
        except:
            pass


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

    def assert_queue(self, queue: str):
        log.info(f"AmqpMock.assert_queue({queue})")
        pass

    def consume(self, queue: str, callback: Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]):
        log.info(f"AmqpMock.consume({queue})")
        pass

    def close(self):
        log.info("AmqpMock.close()")
        pass
