from __future__ import annotations

import math

import pandas as pd

from analytics.zscore import rolling_zscore


def compute_series_stats(series: pd.Series, z_window: int = 200) -> dict[str, float | None]:
    clean = pd.to_numeric(series, errors='coerce').dropna()
    if clean.empty:
        return {
            'last': None,
            'min': None,
            'max': None,
            'p05': None,
            'p95': None,
            'zscore': None,
        }

    z_value = None
    z_series = rolling_zscore(clean, window=max(20, int(z_window)))
    if not z_series.empty:
        non_null_z = z_series.dropna()
        if not non_null_z.empty:
            z_value = float(non_null_z.iloc[-1])
            if not math.isfinite(z_value):
                z_value = None

    return {
        'last': float(clean.iloc[-1]),
        'min': float(clean.min()),
        'max': float(clean.max()),
        'p05': float(clean.quantile(0.05)),
        'p95': float(clean.quantile(0.95)),
        'zscore': z_value,
    }
