from __future__ import annotations

import pandas as pd


def build_spread(pivot_df: pd.DataFrame, contract1: str, contract2: str) -> pd.Series:
    if contract1 not in pivot_df.columns or contract2 not in pivot_df.columns:
        return pd.Series(dtype=float)
    spread = pivot_df[contract1] - pivot_df[contract2]
    spread.name = f'{contract1}-{contract2}'
    return spread


def build_fly(pivot_df: pd.DataFrame, c1: str, c2: str, c3: str) -> pd.Series:
    if any(c not in pivot_df.columns for c in (c1, c2, c3)):
        return pd.Series(dtype=float)
    fly = pivot_df[c1] - 2.0 * pivot_df[c2] + pivot_df[c3]
    fly.name = f'{c1}-2*{c2}+{c3}'
    return fly