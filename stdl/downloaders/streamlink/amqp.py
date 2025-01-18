import asyncio
from typing import Callable, Optional

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
from pika.spec import Basic
from pika.spec import BasicProperties

from stdl.config.env import AmqpConfig


class Amqp:
    def __init__(self, conf: AmqpConfig):
        self.url = f"amqp://{conf.username}:{conf.password}@{conf.host}:{conf.port}"
        self.conn: Optional[AsyncioConnection] = None
        self.ch: Optional[Channel] = None

    async def connect(self) -> tuple[AsyncioConnection, Channel]:
        self.conn = await self.create_connection()
        self.ch = await self.create_channel()
        return self.conn, self.ch

    async def create_connection(self) -> AsyncioConnection:
        future = asyncio.get_event_loop().create_future()

        def on_open(conn: AsyncioConnection):
            future.set_result(conn)

        def on_error(conn: AsyncioConnection, err: BaseException):
            future.set_exception(err)

        AsyncioConnection(
            pika.URLParameters(self.url),
            on_open_callback=on_open,
            on_open_error_callback=on_error,
        )
        return await future

    async def create_channel(self) -> Channel:
        if not self.conn:
            raise ValueError("Connection not created")
        future = asyncio.get_event_loop().create_future()

        def on_open(channel: Channel):
            future.set_result(channel)

        self.conn.channel(on_open_callback=on_open)
        return await future

    def assert_queue(self, queue: str):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.queue_declare(queue=queue, passive=False, durable=False, auto_delete=False, exclusive=False)

    def consume(self, queue: str, callback: Callable[[Channel, Basic.Deliver, BasicProperties, bytes], None]):
        if not self.ch:
            raise ValueError("Channel not created")
        self.ch.basic_consume(queue=queue, on_message_callback=callback)

    def close(self):
        self.conn.close()
