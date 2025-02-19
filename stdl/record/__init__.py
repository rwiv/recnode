import os
import sys

from .platform.recorder_resolver import RecorderResolver
from .recorder.recording_scheduler import RecordingScheduler
from .utils.streamlink_utils import disable_streamlink_log
from .spec.recording_constants import EXIT_QUEUE_PREFIX, DONE_QUEUE_NAME
from .spec.exit_message import ExitMessage, ExitCommand
from .spec.done_message import DoneMessage

targets = ["manager", "platform", "recorder", "spec", "utils"]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
