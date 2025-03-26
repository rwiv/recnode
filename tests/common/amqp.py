import json
import threading
import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pyutils import load_dotenv, path_join, find_project_root, stacktrace

from stdl.common.amqp import AmqpHelperBlocking
from stdl.common.env import get_env
from stdl.record import EXIT_QUEUE_PREFIX

load_dotenv(path_join(find_project_root(), "dev", ".env"))
conf = get_env().amqp
amqp = AmqpHelperBlocking(conf)

uid = "asd"
queue_name = f"{EXIT_QUEUE_PREFIX}.chzzk.{uid}"


def thname():
    return threading.current_thread().name


def publish(chan: BlockingChannel):
    body = json.dumps({"cmd": "cancel", "platform": "chzzk", "uid": uid})
    amqp.publish(chan, queue_name, body.encode("utf-8"))


def test_publish():
    conn, chan = amqp.connect()
    publish(chan)


def test_exists():
    conn, chan = amqp.connect()
    assert amqp.queue_exists(chan, "asdasiod") is False
    conn, chan = amqp.connect()
    assert amqp.queue_exists(chan, "stdl.done") is True


def test_blocking():
    print()
    print(f"[{thname()}] Start")
    conn, chan = amqp.connect()
    amqp.ensure_queue(chan, queue_name, auto_delete=True)

    def wait():
        for i in range(3):
            time.sleep(1)
            publish(chan)
        print(f"[{thname()}] Done")

        conn.add_callback_threadsafe(chan.stop_consuming)

    thread = threading.Thread(target=wait)
    thread.start()

    try:

        def on_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
            content = json.loads(body.decode("utf-8"))
            print(f"[{thname()}] Received: {content}")
            ch.basic_ack(method.delivery_tag)

        amqp.consume(chan, queue_name, on_message)
    except:
        print(f"[{thname()}] Error")
        print(stacktrace())
    amqp.close(conn)
    amqp.close(conn)
    print("Done")
