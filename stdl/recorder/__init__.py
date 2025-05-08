import os
import sys

from .manager.recording_scheduler import RecordingScheduler
from .manager.recorder_resolver import RecorderResolver
from .schema.recording_constants import EXIT_QUEUE_PREFIX, DONE_QUEUE_NAME
from .schema.done_message import DoneMessage, DoneStatus
from .schema.recording_arguments import StreamLinkSessionArgs
from .stream.streamlink_utils import disable_streamlink_log, get_streams

targets = [
    "manger",
    "schema",
    "stream",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
