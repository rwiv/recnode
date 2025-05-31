import os
import sys

from .histogram import Histogram
from .metric_manager import MetricManager, metric

targets = [
    "histogram",
    "metric_manager",
]
if os.getenv("PY_ENV") != "prod":
    for name in list(sys.modules.keys()):
        for target in targets:
            if name.startswith(f"{__name__}.{target}"):
                sys.modules[name] = None  # type: ignore
