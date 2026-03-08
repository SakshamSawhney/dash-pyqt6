from __future__ import annotations

from datetime import datetime
import re

import pandas as pd

_MONTH_CODES = {
    'F': 1,
    'G': 2,
    'H': 3,
    'J': 4,
    'K': 5,
    'M': 6,
    'N': 7,
    'Q': 8,
    'U': 9,
    'V': 10,
    'X': 11,
    'Z': 12,
}


def _instrument_to_maturity_key(instrument: str) -> int | None:
    # Supports symbols ending with month code + 1 or 2 year digits (e.g. ERH6, ERM26).
    match = re.search(r'([FGHJKMNQUVXZ])(\d{1,2})$', instrument.upper())
    if not match:
        return None

    month_code = match.group(1)
    year_part = int(match.group(2))
    year = 2000 + year_part
    month = _MONTH_CODES[month_code]
    return year * 100 + month


def build_curve_points(latest_prices: pd.Series) -> pd.DataFrame:
    rows: list[dict] = []
    fallback_order = 0

    for ins, price in latest_prices.dropna().items():
        maturity_key = _instrument_to_maturity_key(str(ins))
        if maturity_key is not None:
            year = maturity_key // 100
            month = maturity_key % 100
            maturity_dt = datetime(year, month, 1)
            sort_key = maturity_key
        else:
            # Fallback for non-futures naming in Excel data.
            maturity_dt = pd.NaT
            sort_key = 10_000_000 + fallback_order
            fallback_order += 1

        rows.append(
            {
                'instrument': str(ins),
                'maturity': maturity_dt,
                'rate': float(price),
                '_sort_key': sort_key,
            }
        )

    if not rows:
        return pd.DataFrame(columns=['instrument', 'maturity', 'rate'])

    curve = pd.DataFrame(rows).sort_values('_sort_key').drop(columns=['_sort_key']).reset_index(drop=True)
    return curve