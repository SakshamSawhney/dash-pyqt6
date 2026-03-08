from __future__ import annotations

import numpy as np


def safe_last(values) -> float | None:
    if values is None or len(values) == 0:
        return None
    v = values[-1]
    if np.isnan(v):
        return None
    return float(v)