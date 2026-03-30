from __future__ import annotations

import math

import pandas as pd

from analytics.zscore import rolling_zscore


def compute_series_stats(
    series: pd.Series,
    z_window: int = 200,
    lookback_days: int = 30,
) -> dict[str, float | int | None]:
    clean = pd.to_numeric(series, errors='coerce').dropna()
    lookback_days = max(1, int(lookback_days))

    idx = pd.to_datetime(clean.index, utc=True, errors='coerce')
    valid_time_mask = pd.notna(idx)
    if valid_time_mask.any():
        clean = clean[valid_time_mask]
        idx = idx[valid_time_mask]
        cutoff = idx.max() - pd.Timedelta(days=lookback_days)
        clean = clean[idx >= cutoff]

    if clean.empty:
        return {
            'last': None,
            'min': None,
            'max': None,
            'p05': None,
            'p95': None,
            'zscore': None,
            'range_width': None,
            'dist_to_high': None,
            'dist_to_low': None,
            'pct_from_high': None,
            'pct_from_low': None,
            'lookback_days': lookback_days,
            'samples': 0,
        }

    z_value = None
    z_series = rolling_zscore(clean, window=max(20, int(z_window)))
    if not z_series.empty:
        non_null_z = z_series.dropna()
        if not non_null_z.empty:
            z_value = float(non_null_z.iloc[-1])
            if not math.isfinite(z_value):
                z_value = None

    last_value = float(clean.iloc[-1])
    min_value = float(clean.min())
    max_value = float(clean.max())
    range_width = max_value - min_value
    dist_to_high = max_value - last_value
    dist_to_low = last_value - min_value
    pct_from_high = 0.0 if range_width == 0 else dist_to_high / range_width
    pct_from_low = 0.0 if range_width == 0 else dist_to_low / range_width

    return {
        'last': last_value,
        'min': min_value,
        'max': max_value,
        'p05': float(clean.quantile(0.05)),
        'p95': float(clean.quantile(0.95)),
        'zscore': z_value,
        'range_width': float(range_width),
        'dist_to_high': float(dist_to_high),
        'dist_to_low': float(dist_to_low),
        'pct_from_high': float(pct_from_high),
        'pct_from_low': float(pct_from_low),
        'lookback_days': lookback_days,
        'samples': int(len(clean)),
    }
