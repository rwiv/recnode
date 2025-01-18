import asyncio
import json

import pytest
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from stdl.config.env import get_env
from stdl.downloaders.streamlink.amqp import Amqp
from stdl.utils.env import load_env

load_env("../../dev/.env")
amqp_conf = get_env().amqp
queue = "tasks"
amqp = Amqp(amqp_conf)


def on_message(ch: Channel, method: Basic.Deliver, props: BasicProperties, body: bytes):
    content = json.loads(body.decode("utf-8"))
    print(content)
    ch.basic_ack(method.delivery_tag)


def publish(ch: Channel):
    ch.basic_publish(
        exchange="", routing_key=queue,
        body=json.dumps({"message": "hello World!", "author": "john"}),
    )


@pytest.mark.asyncio
async def test_all():
    print()
    await amqp.connect()
    amqp.assert_queue(queue)
    amqp.consume(queue, on_message)

    new_ch = await amqp.create_channel()
    publish(new_ch)

    await asyncio.sleep(1)
    amqp.close()
    await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_consume():
    print()
    await amqp.connect()
    amqp.assert_queue(queue)
    amqp.consume(queue, on_message)
    await asyncio.sleep(10)


@pytest.mark.asyncio
async def test_publish():
    print()
    conn, ch = await amqp.connect()
    amqp.assert_queue(queue)
    publish(ch)
    publish(ch)
    publish(ch)
    publish(ch)
    publish(ch)
    await asyncio.sleep(1)
