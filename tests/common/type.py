import json

from stdl.common.config import read_app_config_by_file, AppConfig


def test_yaml():
    print()
    conf = read_app_config_by_file("../../dev/conf.yaml")
    print(conf)
    a = json.dumps(conf.to_dict())
    print(a)


def test_json():
    print()
    with open("../../dev/test_req.json", "r") as file:
        text = file.read()
    print(json.loads(text))
    conf = AppConfig.from_dict(json.loads(text))
    print(conf)
