from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

API_URL = 'https://qh-api.corp.hertshtengroup.com/api/v2/ohlc/'
API_ACCESS_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoyMDg4Njk1MDM0LCJpYXQiOjE3NzMzMzUwMzQsImp0aSI6IjA2NjY4ZmM4ODI4MTQ5OTZhNzI4YzQxYzQ4NjExMmJiIiwidXNlcl9pZCI6Mzg0fQ.Chqlszy0KdL3QUfcMKXa13P0ZV4OonVWms_R_c2Hifw'
API_PREFIX = 'FERER'
VALID_INTERVALS = ('1M', '5M', '1H', '1D')
RATE_LIMIT_PER_MINUTE = 50
PRICE_SCALE = 100.0
_REQUEST_TIMES: deque[float] = deque()
_RATE_LIMIT_LOCK = threading.Lock()


class HistoricalApiError(RuntimeError):
    pass


def to_api_instrument(instrument: str) -> str:
    code = str(instrument).strip().upper()
    if code.startswith(API_PREFIX):
        return code
    return f'{API_PREFIX}{code}'


def from_api_instrument(product: str) -> str:
    text = str(product).strip().upper()
    if text.startswith(API_PREFIX):
        return text[len(API_PREFIX) :]
    return text


def datetime_to_unix_seconds(value: datetime) -> int:
    dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def fetch_historical_ohlc(
    instruments: Iterable[str],
    interval: str,
    start_unix: int,
    end_unix: int,
    count: int | None = 500,
) -> pd.DataFrame:
    normalized = [to_api_instrument(name) for name in instruments if str(name).strip()]
    if not normalized:
        raise ValueError('At least one instrument is required.')
    if not API_ACCESS_TOKEN.strip():
        raise ValueError('Set API_ACCESS_TOKEN in data/historical_api.py before using the historical API.')
    normalized_interval = str(interval).strip().upper()
    if normalized_interval not in VALID_INTERVALS:
        raise ValueError(f'Interval must be one of: {", ".join(VALID_INTERVALS)}')

    _wait_for_rate_limit()

    params = {
        'instruments': ','.join(normalized),
        'interval': normalized_interval,
    }
    if start_unix is not None:
        params['start'] = str(int(start_unix))
    if end_unix is not None:
        params['end'] = str(int(end_unix))

    provided_window_fields = sum(
        value is not None for value in (start_unix, end_unix, count)
    )
    if provided_window_fields != 2:
        raise ValueError('Provide exactly two of start, end, and count.')
    if not (start_unix is not None and end_unix is not None) and count is not None:
        params['count'] = str(int(count))
    request_url = f'{API_URL}?{urlencode(params)}'
    request = Request(
        request_url,
        headers={
            'Authorization': f'Bearer {API_ACCESS_TOKEN.strip()}',
            'Accept': 'application/json',
            'User-Agent': 'dash-pyqt6/1.0',
        },
        method='GET',
    )

    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except HTTPError as exc:
        body = ''
        try:
            body = exc.read().decode('utf-8', errors='replace').strip()
        except Exception:
            body = ''
        details = body or 'No response body returned.'
        raise HistoricalApiError(
            f'Historical API HTTP {exc.code} for URL:\n{request_url}\n\nResponse:\n{details}'
        ) from exc
    except URLError as exc:
        raise HistoricalApiError(f'Historical API connection error: {exc}') from exc
    except json.JSONDecodeError as exc:
        raise HistoricalApiError(f'Historical API returned invalid JSON for URL:\n{request_url}') from exc

    if not isinstance(payload, list):
        raise HistoricalApiError(f'Historical API response was not a list for URL:\n{request_url}')

    rows: list[dict] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        timestamp_ms = item.get('time')
        close = item.get('close')
        if timestamp_ms in (None, '') or close in (None, ''):
            continue
        timestamp = pd.to_datetime(timestamp_ms, unit='ms', utc=True, errors='coerce')
        price = pd.to_numeric(close, errors='coerce')
        volume = pd.to_numeric(item.get('volume', 0.0), errors='coerce')
        if pd.isna(timestamp) or pd.isna(price):
            continue
        rows.append(
            {
                'timestamp': timestamp,
                'instrument': from_api_instrument(str(item.get('product', ''))),
                'price': float(price) * PRICE_SCALE,
                'bid': float(price) * PRICE_SCALE,
                'ask': float(price) * PRICE_SCALE,
                'volume': 0.0 if pd.isna(volume) else float(volume),
            }
        )

    if not rows:
        return pd.DataFrame(columns=['timestamp', 'instrument', 'price', 'bid', 'ask', 'volume'])

    return pd.DataFrame(rows).sort_values('timestamp').reset_index(drop=True)


def _wait_for_rate_limit() -> None:
    while True:
        with _RATE_LIMIT_LOCK:
            now = time.monotonic()
            window_start = now - 60.0
            while _REQUEST_TIMES and _REQUEST_TIMES[0] < window_start:
                _REQUEST_TIMES.popleft()
            if len(_REQUEST_TIMES) < RATE_LIMIT_PER_MINUTE:
                _REQUEST_TIMES.append(now)
                return
            sleep_for = max(0.05, 60.0 - (now - _REQUEST_TIMES[0]))
        time.sleep(sleep_for)
