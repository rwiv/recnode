import json
from os.path import join

from stdl.common.request_config import read_app_config_by_file, AppConfig
from stdl.utils.path import find_project_root


def test_yaml():
    print()
    conf = read_app_config_by_file(join(find_project_root(), "dev", "conf.yaml"))
    print(conf)


def test_json():
    print()
    with open(join(find_project_root(), "dev", "test_req.json"), "r") as file:
        text = file.read()
    print(json.loads(text))
    conf = AppConfig(**json.loads(text))
    print(conf)
