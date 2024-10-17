import json
from dataclasses import asdict, is_dataclass

from stdl.config.config import read_app_config_by_file, read_app_config_by_event


def test_by_file():
    print()
    conf = read_app_config_by_file("../../dev/conf.yaml")
    if is_dataclass(conf):
        conf = asdict(conf)
    a = json.dumps(conf)
    print(a)


def test_by_event():
    print()
    conf = read_app_config_by_event("../../dev/event.json")
    print(conf.reqType)
    print(conf.chzzkLive)


def test():
    try:
        return read_app_config_by_event("/etc/jobsink-event/event")
    except FileNotFoundError:
        return read_app_config_by_file("asdasdsad")
