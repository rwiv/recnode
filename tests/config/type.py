import json
from dataclasses import asdict, is_dataclass

from stdl.common.config import read_app_config_by_file


def test_by_file():
    print()
    conf = read_app_config_by_file("../../dev/conf.yaml")
    if is_dataclass(conf):
        conf = asdict(conf)
    a = json.dumps(conf)
    print(a)
