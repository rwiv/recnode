import logging


def disable_streamlink_log():
    logging.getLogger("streamlink").setLevel(logging.CRITICAL)

