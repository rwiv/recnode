import stdl.chzzk_vid.app as chzzk_vid
from stdl.config.config import read_app_config
from stdl.config.requests import RequestType


def test_chzzk():
    print()
    conf = read_app_config("../../dev/conf.yaml")
    print(conf.req_type() == RequestType.CHZZK_VID)
    # chzzk_vid.run(conf.chzzkVideo.videoNo)
