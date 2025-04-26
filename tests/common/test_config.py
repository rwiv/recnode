from pyutils import path_join, find_project_root

from stdl.common.request import read_request_by_file, AppRequest


def test_yaml():
    print()
    conf = read_request_by_file(path_join(find_project_root(), "dev", "conf.yaml"))
    print(conf)
