import sys

from .platform.soop_types import SoopCredential
from .platform.recorder_resolver import RecorderResolver
from .recorder.recording_scheduler import RecordingScheduler
from .utils.streamlink_utils import disable_streamlink_log

targets = ["manager", "platform", "recorder", "spec", "utils"]
for name in list(sys.modules.keys()):
    for target in targets:
        if name.startswith(f"{__name__}.{target}"):
            sys.modules[name] = None  # type: ignore
