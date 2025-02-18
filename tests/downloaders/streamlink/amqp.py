import json

import requests
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from stdl.common.amqp import AmqpHelperBlocking
from stdl.common.request_config import read_app_config_by_file, AppConfig
from stdl.common.env import get_env
from stdl.common.request_types import RequestType
from stdl.common.types import PlatformType, FsType
from stdl.downloaders.streamlink.listener import EXIT_QUEUE_PREFIX
from stdl.downloaders.streamlink.recorder import DONE_QUEUE_NAME
from stdl.event.exit_message import ExitMessage, ExitCommand
from stdl.server.main_router import CancelRequest
from stdl.utils.env import load_env
from stdl.utils.path import find_project_root, path_join

load_env(path_join(find_project_root(), "dev", ".env"))
amqp_conf = get_env().amqp

conf = read_app_config_by_file(path_join(find_project_root(), "dev", "conf.yaml"))

if conf.chzzk_live is None:
    raise ValueError("Config not found")

uid = conf.chzzk_live.uid
exit_queue_name = f"{EXIT_QUEUE_PREFIX}.chzzk.{uid}"
amqp = AmqpHelperBlocking(amqp_conf)


def test_post_record():
    print()
    res = requests.post(
        "http://localhost:9083/api/recordings",
        json=AppConfig(
            fsType=FsType.LOCAL,
            reqType=RequestType.CHZZK_LIVE,
            chzzkLive=conf.chzzk_live,
        ).model_dump(by_alias=True, mode="json"),
    )
    print(res.text)


def test_delete_record():
    print()
    res = requests.delete(
        "http://localhost:9083/api/recordings",
        json=CancelRequest(
            platformType=PlatformType.CHZZK,
            uid=uid,
        ).model_dump(by_alias=True, mode="json"),
    )
    print(res.text)


def test_exit_publish():
    print()
    conn, chan = amqp.connect()
    amqp.ensure_queue(chan, exit_queue_name, auto_delete=True)
    msg = ExitMessage(
        cmd=ExitCommand.CANCEL,
        # cmd=ExitCommand.FINISH,
        uid=uid,
        platform=PlatformType.CHZZK,
    ).model_dump_json(by_alias=True)
    body = msg.encode("utf-8")
    amqp.publish(chan, exit_queue_name, body)
    amqp.close(conn)


def test_done_consume():
    print()
    conn, chan = amqp.connect()
    amqp.ensure_queue(chan, DONE_QUEUE_NAME, auto_delete=False)

    def on_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        print(json.loads(body.decode("utf-8")))
        ch.basic_ack(method.delivery_tag)
        ch.stop_consuming()

    amqp.consume(chan, DONE_QUEUE_NAME, on_message)
    amqp.close(conn)
