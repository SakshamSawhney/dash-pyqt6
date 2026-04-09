from __future__ import annotations

import math

import numpy as np
import pandas as pd


def estimate_half_life(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) < 20:
        return None

    lagged = clean.shift(1).dropna()
    delta = clean.diff().dropna()
    aligned = pd.concat({"lagged": lagged, "delta": delta}, axis=1).dropna()
    if len(aligned) < 20:
        return None

    x = aligned["lagged"].astype(float)
    y = aligned["delta"].astype(float)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denom = float(((x - x_mean) ** 2).sum())
    if math.isclose(denom, 0.0):
        return None
    beta = float(((x - x_mean) * (y - y_mean)).sum() / denom)
    if not np.isfinite(beta) or beta >= 0.0:
        return None
    half_life = -math.log(2.0) / beta
    if not np.isfinite(half_life) or half_life <= 0.0:
        return None
    return float(half_life)
