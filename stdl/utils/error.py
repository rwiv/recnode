import sys
import traceback


def stacktrace():
    exc_info = sys.exc_info()
    trace = traceback.format_exception(*exc_info)
    return "".join(trace)
