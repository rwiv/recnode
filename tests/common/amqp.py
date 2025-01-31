import json
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import AmqpBlocking
from stdl.common.env import get_env
from stdl.downloaders.streamlink.listener import EXIT_QUEUE_PREFIX
from stdl.utils.env import load_env

load_env("../../dev/.env")
conf = get_env().amqp
amqp = AmqpBlocking(conf)

uid = "asd"
# TODO: change `:` to `.`
queue_name = f"{EXIT_QUEUE_PREFIX}:chzzk:{uid}"


def on_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
    print(json.loads(body.decode("utf-8")))
    ch.basic_ack(method.delivery_tag)


def publish(client: AmqpBlocking):
    body = json.dumps({
        "cmd": "cancel",
        "platform": "chzzk",
        "uid": uid
    })
    client.publish(queue_name, body.encode("utf-8"))


def test_publish():
    amqp.connect()
    publish(amqp)


def test_blocking():
    print()
    conn, ch = amqp.connect()
    amqp.assert_queue(queue_name, auto_delete=True)

    def wait():
        for i in range(5):
            time.sleep(1)
            publish(amqp)
        ch.stop_consuming()
    thread = threading.Thread(target=wait)
    thread.Daemon = True
    thread.start()

    try:
        amqp.consume(queue_name, on_message)
    except Exception as e:
        print(e)
    print("Done")
