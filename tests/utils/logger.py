import logging

from stdl.utils.logger import log, get_error_info


def test_logger():
    print()
    log.is_prod = False
    # log.is_prod = True
    log.set_level(logging.INFO)
    log.info("Hello World")
    log.info("Test Log", {
        "foo": "bar",
        "hello": "world",
    })


def test_ex():
    print()
    log.is_prod = False
    # log.is_prod = True
    try:
        raise Exception("foo", "bar")
    except (Exception,):
        log.error(*get_error_info())
