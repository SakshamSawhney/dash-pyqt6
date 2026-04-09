from __future__ import annotations

import threading
from collections import deque
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from PyQt6.QtCore import QObject, pyqtSignal

COLUMNS = ['timestamp', 'instrument', 'price', 'bid', 'ask', 'open', 'high', 'low', 'close', 'volume']


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

    years = parsed.dt.year
    parsed = parsed.where((years >= 1990) & (years <= 2100))
    return parsed


class MarketDataStore(QObject):
    data_appended = pyqtSignal(int)

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._data = pd.DataFrame(columns=COLUMNS)
        self._live_latest: dict[str, dict[str, Any]] = {}
        self._live_events: deque[dict[str, Any]] = deque(maxlen=4000)

    @staticmethod
    def _normalize_live_price_mode(mode: str) -> str:
        return 'vwap' if str(mode).strip().lower() == 'vwap' else 'last'

    def set_use_live_mid_price(self, enabled: bool) -> None:
        return

    def set_live_price_mode(self, mode: str) -> None:
        return

    def load_historical(self, historical_df: pd.DataFrame) -> None:
        self.append_batch(historical_df)

    def append_tick(self, tick: dict[str, Any]) -> None:
        scale = 100.0
        ts_value = tick.get('timestamp')
        raw_price = _to_float(tick.get('price'), 0.0)
        raw_bid = _to_float(tick.get('bid'), np.nan)
        raw_ask = _to_float(tick.get('ask'), np.nan)
        raw_bid_qty = _to_float(tick.get('bid_qty'), np.nan)
        raw_ask_qty = _to_float(tick.get('ask_qty'), np.nan)
        raw_last_qty = _to_float(tick.get('last_qty'), np.nan)
        direction = str(tick.get('direction') or '').strip()

        last_price = raw_price * scale
        vwap_price = np.nan
        if (
            np.isfinite(raw_bid)
            and np.isfinite(raw_ask)
            and np.isfinite(raw_bid_qty)
            and np.isfinite(raw_ask_qty)
            and (raw_bid_qty + raw_ask_qty) > 0.0
        ):
            vwap = ((raw_ask * raw_bid_qty) + (raw_bid * raw_ask_qty)) / (raw_bid_qty + raw_ask_qty)
            vwap_price = vwap * scale

        bid = (raw_bid if np.isfinite(raw_bid) else raw_price) * scale
        ask = (raw_ask if np.isfinite(raw_ask) else raw_price) * scale
        volume = _to_float(tick.get('volume'), 0.0)
        instrument = str(tick.get('instrument') or '').strip()
        if not instrument:
            return

        with self._lock:
            self._live_latest[instrument] = {
                'timestamp': ts_value,
                'last_price': float(last_price),
                'vwap_price': float(vwap_price) if np.isfinite(vwap_price) else float(last_price),
                'bid': float(bid),
                'ask': float(ask),
                'bid_qty': float(raw_bid_qty) if np.isfinite(raw_bid_qty) else 0.0,
                'ask_qty': float(raw_ask_qty) if np.isfinite(raw_ask_qty) else 0.0,
                'last_qty': float(raw_last_qty) if np.isfinite(raw_last_qty) else 0.0,
                'direction': direction,
                'open': float(last_price),
                'high': float(last_price),
                'low': float(last_price),
                'close': float(last_price),
                'volume': float(volume),
            }
            self._live_events.append(
                {
                    'timestamp': ts_value,
                    'instrument': instrument,
                    'price': float(last_price),
                    'bid': float(bid),
                    'ask': float(ask),
                    'bid_qty': float(raw_bid_qty) if np.isfinite(raw_bid_qty) else 0.0,
                    'ask_qty': float(raw_ask_qty) if np.isfinite(raw_ask_qty) else 0.0,
                    'last_qty': float(raw_last_qty) if np.isfinite(raw_last_qty) else 0.0,
                    'direction': direction,
                    'volume': float(volume),
                }
            )

    def append_batch(self, batch_df: pd.DataFrame) -> None:
        if batch_df.empty:
            return

        df = batch_df.copy()
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = 0.0 if col in ('volume', 'bid', 'ask', 'price', 'open', 'high', 'low', 'close') else None
        df = df[COLUMNS]

        df['timestamp'] = _normalize_timestamp_series(df['timestamp'])
        df = df.dropna(subset=['timestamp', 'instrument'])
        if df.empty:
            return

        with self._lock:
            self._data = pd.concat([self._data, df], ignore_index=True)
            self._data = self._data.sort_values('timestamp').reset_index(drop=True)
            self._data = self._data.drop_duplicates(subset=['timestamp', 'instrument'], keep='last').reset_index(drop=True)
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

    def get_price_pivot(
        self,
        instruments: list[str] | None = None,
        include_live: bool = True,
        live_price_mode: str = 'last',
    ) -> pd.DataFrame:
        with self._lock:
            if self._data.empty:
                df = pd.DataFrame(columns=['timestamp', 'instrument', 'price'])
            else:
                df = self._data[['timestamp', 'instrument', 'price']].copy()
            live_latest = dict(self._live_latest)

        if instruments:
            df = df[df['instrument'].isin(instruments)]

        if df.empty:
            pivot = pd.DataFrame()
        else:
            pivot = (
                df.pivot_table(index='timestamp', columns='instrument', values='price', aggfunc='last')
                .sort_index()
                .ffill()
            )
            pivot.columns = [str(c) for c in pivot.columns]

        live_series_data: dict[str, float] = {}
        live_timestamps = []
        price_key = 'vwap_price' if self._normalize_live_price_mode(live_price_mode) == 'vwap' else 'last_price'
        for ins, payload in live_latest.items():
            if instruments and ins not in instruments:
                continue
            price = _to_float(payload.get(price_key), default=np.nan)
            if np.isfinite(price):
                live_series_data[ins] = float(price)
            ts = pd.to_datetime(payload.get('timestamp'), utc=True, errors='coerce')
            if pd.notna(ts):
                live_timestamps.append(ts)

        if include_live and live_series_data:
            snapshot_ts = max(live_timestamps) if live_timestamps else pd.Timestamp.now(tz='UTC')
            if not pivot.empty:
                last_ts = pd.to_datetime(pivot.index.max(), utc=True, errors='coerce')
                if pd.notna(last_ts):
                    snapshot_ts = max(snapshot_ts, last_ts + pd.Timedelta(microseconds=1))
            live_row = pd.DataFrame([live_series_data], index=[snapshot_ts])
            pivot = pd.concat([pivot, live_row], axis=0, sort=False).sort_index().ffill()

        if pivot.empty:
            return pd.DataFrame()

        if instruments:
            ordered_cols = [str(ins) for ins in instruments if str(ins) in pivot.columns]
            if ordered_cols:
                pivot = pivot.reindex(columns=ordered_cols)
        return pivot

    def get_ohlc(self, instrument: str, include_live: bool = True, live_price_mode: str = 'last') -> pd.DataFrame:
        name = str(instrument).strip()
        if not name:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close'])

        with self._lock:
            if self._data.empty:
                df = pd.DataFrame(columns=['timestamp', 'instrument', 'open', 'high', 'low', 'close'])
            else:
                df = self._data[['timestamp', 'instrument', 'open', 'high', 'low', 'close']].copy()
            live_latest = dict(self._live_latest.get(name, {}))

        if not df.empty:
            df = df[df['instrument'] == name].copy()
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            df = df.dropna(subset=['timestamp']).sort_values('timestamp')
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            ohlc = df.set_index('timestamp')[['open', 'high', 'low', 'close']]
        else:
            ohlc = pd.DataFrame(columns=['open', 'high', 'low', 'close'])

        if include_live and live_latest:
            live_ts = pd.to_datetime(live_latest.get('timestamp'), utc=True, errors='coerce')
            if pd.notna(live_ts):
                if not ohlc.empty:
                    last_ts = pd.to_datetime(ohlc.index.max(), utc=True, errors='coerce')
                    if pd.notna(last_ts):
                        live_ts = max(live_ts, last_ts + pd.Timedelta(microseconds=1))
                price_key = 'vwap_price' if self._normalize_live_price_mode(live_price_mode) == 'vwap' else 'last_price'
                live_price = _to_float(live_latest.get(price_key), default=np.nan)
                if np.isfinite(live_price):
                    live_row = pd.DataFrame(
                        [{'open': live_price, 'high': live_price, 'low': live_price, 'close': live_price}],
                        index=[live_ts],
                    )
                    ohlc = pd.concat([ohlc, live_row], axis=0).sort_index()

        return ohlc

    def get_available_history_dates(self) -> list[str]:
        with self._lock:
            if self._data.empty:
                return []
            ts = pd.to_datetime(self._data['timestamp'], utc=True, errors='coerce')
        ts = ts[pd.notna(ts)]
        if ts.empty:
            return []
        days = sorted({d.strftime('%Y-%m-%d') for d in ts.dt.tz_convert('UTC').dt.date})
        return days

    def get_latest_history_timestamp(self) -> pd.Timestamp | None:
        with self._lock:
            if self._data.empty:
                return None
            ts = pd.to_datetime(self._data['timestamp'], utc=True, errors='coerce')
        ts = ts[pd.notna(ts)]
        if ts.empty:
            return None
        return ts.max()

    def get_live_snapshot_timestamp(self) -> pd.Timestamp | None:
        with self._lock:
            if not self._live_latest:
                return None
            values = list(self._live_latest.values())
        timestamps = pd.to_datetime([v.get('timestamp') for v in values], utc=True, errors='coerce')
        timestamps = timestamps[pd.notna(timestamps)]
        if len(timestamps) == 0:
            return None
        return timestamps.max()

    def get_live_latest_snapshot(self) -> pd.DataFrame:
        with self._lock:
            if not self._live_latest:
                return pd.DataFrame()
            rows = [{'instrument': instrument, **payload} for instrument, payload in self._live_latest.items()]
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        for col in ['last_price', 'vwap_price', 'bid', 'ask', 'bid_qty', 'ask_qty', 'last_qty', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.sort_values(['timestamp', 'instrument']).reset_index(drop=True)

    def get_live_event_frame(self, max_age_seconds: int = 15) -> pd.DataFrame:
        with self._lock:
            rows = list(self._live_events)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df = df.dropna(subset=['timestamp', 'instrument'])
        if df.empty:
            return df
        for col in ['price', 'bid', 'ask', 'bid_qty', 'ask_qty', 'last_qty', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(seconds=max(1, int(max_age_seconds)))
        df = df[df['timestamp'] >= cutoff]
        return df.sort_values('timestamp').reset_index(drop=True)

    def persist_parquet(self, path: Path) -> None:
        df = self.get_data()
        if df.empty:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
