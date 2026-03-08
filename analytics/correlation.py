from __future__ import annotations

import pandas as pd


def correlation_matrix(pivot_df: pd.DataFrame, instruments: list[str]) -> pd.DataFrame:
    selected = [ins for ins in instruments if ins in pivot_df.columns]
    if len(selected) < 2:
        return pd.DataFrame()
    return pivot_df[selected].corr()