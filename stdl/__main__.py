from stdl.config.config import read_app_config
from stdl.config.requests import RequestType

if __name__ == "__main__":
    conf = read_app_config("../dev/conf.yaml")
    print(conf.req_type() == RequestType.CHZZK_VID)
