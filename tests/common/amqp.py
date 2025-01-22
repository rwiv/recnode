import json
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import AmqpBlocking
from stdl.common.env import get_env
from stdl.utils.env import load_env

load_env("../../dev/.env")
conf = get_env().amqp
queue = "tasks"
amqp = AmqpBlocking(conf)


def on_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
    content = json.loads(body.decode("utf-8"))
    print(type(ch))
    print(content)
    ch.basic_ack(method.delivery_tag)


def publish(ch: BlockingChannel):
    ch.basic_publish(
        exchange="", routing_key=queue,
        body=json.dumps({"message": "hello World!", "author": "john"}),
    )


def test_blocking():
    print()
    conn, ch = amqp.connect()
    amqp.assert_queue(queue)

    def wait():
        for i in range(10):
            time.sleep(1)
            publish(ch)
        ch.stop_consuming()
    thread = threading.Thread(target=wait)
    thread.Daemon = True
    thread.start()

    amqp.consume(queue, on_message)
    print("Done")
