import json

from stdl.common.request_config import read_app_config_by_file, AppConfig


def test_yaml():
    print()
    conf = read_app_config_by_file("../../dev/conf.yaml")
    print(conf)
    a = json.dumps(conf.model_dump(mode="json"))
    print(a)


def test_json():
    print()
    with open("../../dev/test_req.json", "r") as file:
        text = file.read()
    print(json.loads(text))
    conf = AppConfig(**json.loads(text))
    print(conf)
