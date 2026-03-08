from __future__ import annotations

import numpy as np
import pandas as pd


def rolling_zscore(series: pd.Series, window: int = 100) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)

    s = pd.to_numeric(series, errors='coerce').astype(float)
    min_periods = min(max(5, window // 10), window)

    rolling_mean = s.rolling(window=window, min_periods=min_periods).mean()
    rolling_std = s.rolling(window=window, min_periods=min_periods).std(ddof=0)

    zscore = (s - rolling_mean) / rolling_std.replace(0.0, np.nan)
    return zscore.astype(float)