import json

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import AmqpBlocking
from stdl.common.config import read_app_config_by_file
from stdl.common.env import get_env
from stdl.common.types import PlatformType
from stdl.downloaders.streamlink.listener import EXIT_QUEUE_PREFIX
from stdl.downloaders.streamlink.recorder import DONE_QUEUE_NAME
from stdl.event.exit_message import ExitMessage, ExitCommand
from stdl.utils.env import load_env

load_env("../../../dev/.env")
amqp_conf = get_env().amqp

conf = read_app_config_by_file("../../../dev/conf.yaml")

uid = conf.chzzkLive.uid
# TODO: change `:` to `.`
exit_queue_name = f"{EXIT_QUEUE_PREFIX}:chzzk:{uid}"


def test_exit_publish():
    print()
    amqp = AmqpBlocking(amqp_conf)
    amqp.connect()
    amqp.assert_queue(exit_queue_name, auto_delete=True)
    body = json.dumps(ExitMessage(
        # cmd=ExitCommand.CANCEL,
        cmd=ExitCommand.FINISH,
        uid=uid,
        platform=PlatformType.CHZZK,
    ).model_dump(mode="json")).encode("utf-8")
    amqp.publish(exit_queue_name, body)
    amqp.close()


def test_done_consume():
    print()
    amqp = AmqpBlocking(amqp_conf)
    amqp.connect()
    amqp.assert_queue(DONE_QUEUE_NAME, auto_delete=False)

    def on_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        print(json.loads(body.decode("utf-8")))
        ch.basic_ack(method.delivery_tag)
        ch.stop_consuming()
    amqp.consume(DONE_QUEUE_NAME, on_message)
    amqp.close()
