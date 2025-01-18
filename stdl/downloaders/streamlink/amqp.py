import asyncio
from typing import Callable, Optional

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
from pika.spec import Basic
from pika.spec import BasicProperties

from stdl.config.env import AmqpConfig


class Amqp:
    def __init__(self, queue: str, conf: AmqpConfig):
        self.queue = queue
        self.url = f"amqp://{conf.username}:{conf.password}@{conf.host}:{conf.port}"
        self.conn: Optional[AsyncioConnection] = None
        self.ch: Optional[Channel] = None

    async def connect(self) -> AsyncioConnection:
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
        self.conn = await future
        return self.conn

    async def create_channel(self) -> Channel:
        future = asyncio.get_event_loop().create_future()

        def on_open(channel: Channel):
            future.set_result(channel)

        self.conn.channel(on_open_callback=on_open)
        self.ch = await future
        return self.ch

    async def consume(self, callback: Callable[[Channel, Basic.Deliver, BasicProperties, bytes], None]):
        self.ch = await self.create_channel()
        self.ch.queue_declare(queue=self.queue)
        self.ch.basic_consume(queue=self.queue, on_message_callback=callback)

    def close(self):
        self.conn.close()
