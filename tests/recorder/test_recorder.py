import requests
from pyutils import load_dotenv, path_join, find_project_root

from stdl.app import CancelRequest
from stdl.common.request import read_request_by_file, AppRequest, RequestType
from stdl.common.spec import PlatformType

load_dotenv(path_join(find_project_root(), "dev", ".env"))

conf = read_request_by_file(path_join(find_project_root(), "dev", "conf.yaml"))

if conf.chzzk_live is None:
    raise ValueError("Config not found")

uid = conf.chzzk_live.uid

worker_url = "http://localhost:9083/api/recordings"


def test_post_record():
    print()
    res = requests.post(
        worker_url,
        json=AppRequest(
            reqType=RequestType.CHZZK_LIVE,
            chzzkLive=conf.chzzk_live,
        ).model_dump(by_alias=True, mode="json"),
    )
    print(res.text)


def test_delete_record():
    print()
    res = requests.delete(
        worker_url,
        json=CancelRequest(
            platform=PlatformType.CHZZK,
            uid=uid,
        ).model_dump(by_alias=True, mode="json"),
    )
    print(res.text)
