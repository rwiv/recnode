import json

from stdl.common import read_app_config_by_file, AppConfig
from stdl.utils.path import find_project_root, path_join


def test_yaml():
    print()
    conf = read_app_config_by_file(path_join(find_project_root(), "dev", "conf.yaml"))
    print(conf)


def test_json():
    print()
    with open(path_join(find_project_root(), "dev", "test_req.json"), "r") as file:
        text = file.read()
    print(json.loads(text))
    conf = AppConfig(**json.loads(text))
    print(conf)
