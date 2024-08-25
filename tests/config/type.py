import json
from dataclasses import asdict, is_dataclass

from stdl.config.config import read_app_config


def test():
    print()
    conf = read_app_config("../../dev/conf.yaml")
    if is_dataclass(conf):
        conf = asdict(conf)
    a = json.dumps(conf)
    print(a)
