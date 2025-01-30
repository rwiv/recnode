from enum import Enum


class PlatformType(Enum):
    CHZZK = "chzzk"
    SOOP = "soop"
    TWITCH = "twitch"


class FsType(Enum):
    LOCAL = "local"
    S3 = "s3"
