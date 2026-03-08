from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from PyQt6.QtCore import QObject, pyqtSignal

COLUMNS = ['timestamp', 'instrument', 'price', 'bid', 'ask', 'volume']


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ''):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=True, errors='coerce', dayfirst=True)

    numeric = pd.to_numeric(series, errors='coerce')
    num_mask = numeric.notna()
    if num_mask.any():
        num_values = numeric[num_mask]
        candidates: list[pd.Series] = [
            pd.to_datetime(num_values, unit='s', utc=True, errors='coerce'),
            pd.to_datetime(num_values, unit='ms', utc=True, errors='coerce'),
            pd.to_datetime(num_values, unit='us', utc=True, errors='coerce'),
            pd.to_datetime(num_values, unit='ns', utc=True, errors='coerce'),
            pd.to_datetime(num_values, unit='D', origin='1899-12-30', utc=True, errors='coerce'),
        ]

        best = None
        best_score = -1.0
        for cand in candidates:
            valid = cand.dropna()
            if valid.empty:
                continue
            years = valid.dt.year
            score = ((years >= 1990) & (years <= 2100)).mean()
            if score > best_score:
                best = cand
                best_score = score

        if best is not None and best_score >= 0.5:
            parsed.loc[num_mask] = best

    # Final plausibility gate to prevent malformed 1970/invalid points from entering charts.
    years = parsed.dt.year
    parsed = parsed.where((years >= 1990) & (years <= 2100))
    return parsed


class MarketDataStore(QObject):
    data_appended = pyqtSignal(int)

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._data = pd.DataFrame(columns=COLUMNS)

    def load_historical(self, historical_df: pd.DataFrame) -> None:
        self.append_batch(historical_df)

    def append_tick(self, tick: dict[str, Any]) -> None:
        price = _to_float(tick.get('price'), 0.0)
        row = {
            'timestamp': tick.get('timestamp'),
            'instrument': tick.get('instrument'),
            'price': price,
            'bid': _to_float(tick.get('bid'), price),
            'ask': _to_float(tick.get('ask'), price),
            'volume': _to_float(tick.get('volume'), 0.0),
        }
        self.append_batch(pd.DataFrame([row], columns=COLUMNS))

    def append_batch(self, batch_df: pd.DataFrame) -> None:
        if batch_df.empty:
            return

        df = batch_df.copy()
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = 0.0 if col in ('volume', 'bid', 'ask', 'price') else None
        df = df[COLUMNS]

        df['timestamp'] = _normalize_timestamp_series(df['timestamp'])
        df = df.dropna(subset=['timestamp', 'instrument'])
        if df.empty:
            return

        with self._lock:
            self._data = pd.concat([self._data, df], ignore_index=True)
            self._data = self._data.sort_values('timestamp').reset_index(drop=True)
        self.data_appended.emit(len(df))

    def get_data(self) -> pd.DataFrame:
        with self._lock:
            return self._data.copy()

    def get_latest_table(self, limit: int = 200) -> pd.DataFrame:
        with self._lock:
            if self._data.empty:
                return self._data.copy()
            latest = self._data.sort_values('timestamp').tail(limit)
            return latest.reset_index(drop=True)

    def get_instruments(self) -> list[str]:
        with self._lock:
            if self._data.empty:
                return []
            return sorted(self._data['instrument'].dropna().astype(str).unique().tolist())

    def get_price_pivot(self, instruments: list[str] | None = None) -> pd.DataFrame:
        with self._lock:
            if self._data.empty:
                return pd.DataFrame()
            df = self._data[['timestamp', 'instrument', 'price']].copy()

        if instruments:
            df = df[df['instrument'].isin(instruments)]
        if df.empty:
            return pd.DataFrame()

        pivot = (
            df.pivot_table(index='timestamp', columns='instrument', values='price', aggfunc='last')
            .sort_index()
            .ffill()
        )
        pivot.columns = [str(c) for c in pivot.columns]
        return pivot

    def persist_parquet(self, path: Path) -> None:
        df = self.get_data()
        if df.empty:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)