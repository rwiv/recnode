import asyncio
import json

import pytest
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from stdl.config.env import get_env
from stdl.downloaders.streamlink.amqp import Amqp
from stdl.utils.env import load_env


@pytest.mark.asyncio
async def test_amqp():
    print()
    load_env("../../dev/.env")
    amqp_conf = get_env().amqp

    queue = "tasks"

    amqp = Amqp(queue, amqp_conf)
    await amqp.connect()

    def on_message(ch: Channel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        content = json.loads(body.decode("utf-8"))
        print(content["message"])
        print(content["author"])
        ch.basic_ack(method.delivery_tag)
    await amqp.consume(on_message)

    ch_w = await amqp.create_channel()
    ch_w.queue_declare(queue=queue)
    ch_w.basic_publish(
        exchange="", routing_key=queue,
        body=json.dumps({"message": "hello World!", "author": "john"}),
    )

    await asyncio.sleep(3)

    amqp.close()
