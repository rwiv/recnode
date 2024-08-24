from stdl.config.config import AppConfig
from dacite import from_dict


def test():
    print()
    d = {"type": "chzzk"}
    conf = from_dict(AppConfig, d)
    print(conf)
