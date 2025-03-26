import json

from pyutils import path_join, find_project_root

from stdl.common.request import read_request_by_file, AppRequest


def test_yaml():
    print()
    conf = read_request_by_file(path_join(find_project_root(), "dev", "conf.yaml"))
    print(conf)


def test_json():
    print()
    with open(path_join(find_project_root(), "dev", "test_req.json"), "r") as file:
        text = file.read()
    print(json.loads(text))
    req = AppRequest(**json.loads(text))
    print(req)
