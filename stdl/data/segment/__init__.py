import os
import sys

from .seg_num_set import SegmentNumberSet
from .seg_state_service import SegmentStateService, SegmentState


targets = [
    "seg_num_set",
    "seg_state_service",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
