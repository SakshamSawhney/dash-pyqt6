from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from PyQt6.QtCore import QThread, pyqtSignal

try:
    from lightstreamer.client import LightstreamerClient, Subscription
except Exception:  # noqa: BLE001
    LightstreamerClient = None
    Subscription = None


SERVER_URL = 'https://ls-md.corp.hertshtengroup.com/'
ADAPTER_SET = 'TTsdkLSAdapter'
DATA_ADAPTER = 'HGL1_Adapter'

FIELD_NAMES = [
    'command',
    'Exchange',
    'Contract',
    'Product',
    'InstrumentId',
    'ClientRecvTime',
    'ExchangeRecvTime',
    'ServerRecvTime',
    'Open',
    'High',
    'Low',
    'Close',
    'Volume',
    'Last',
    'LastQty',
    'SeriesStatus',
    'Settle',
    'PrevSettle',
    'BestAsk',
    'BestAskQty',
    'BestBid',
    'BestBidQty',
    'IndSettle',
    'Price',
    'AdminPrice',
    'Admin',
    'Direction',
]

SUBSCRIBED_ITEMS = [
    '10924837127710696314',
    '11285112926832882282',
    '11763523448513210914',
    '13214711165974585185',
    '1441063560409485207',
    '1588166901584120091',
    '1669949533877663218',
    '16790086843721301972',
    '17145206416583072881',
    '17673088545712512776',
    '17683132835724770535',
    '18441658785607986399',
    '3837812046146387117',
    '438158120670779055',
    '5338536882455330871',
    '6659523499070145958',
    '6716223349220955538',
    '6956224988117060596',
    '9491949494916341473',
]


def _safe_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_plausible(dt: datetime) -> bool:
    return 2000 <= dt.year <= 2100


def _candidate_dt_from_numeric(value: int) -> list[datetime]:
    return [
        datetime.fromtimestamp(value, tz=timezone.utc),
        datetime.fromtimestamp(value / 1_000.0, tz=timezone.utc),
        datetime.fromtimestamp(value / 1_000_000.0, tz=timezone.utc),
        datetime.fromtimestamp(value / 1_000_000_000.0, tz=timezone.utc),
    ]


def _parse_timestamp(update) -> str:
    now = datetime.now(timezone.utc)

    for key in ('ClientRecvTime', 'ExchangeRecvTime', 'ServerRecvTime'):
        raw = update.getValue(key)
        if raw in (None, ''):
            continue

        text = str(raw).strip()
        if text.isdigit() or (text.startswith('-') and text[1:].isdigit()):
            value = int(text)
            candidates: list[datetime] = []
            try:
                candidates = _candidate_dt_from_numeric(value)
            except Exception:
                candidates = []

            plausible = [dt for dt in candidates if _is_plausible(dt)]
            if plausible:
                chosen = min(plausible, key=lambda dt: abs((dt - now).total_seconds()))
                return chosen.isoformat()
            continue

        try:
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            if _is_plausible(dt):
                return dt.isoformat()
        except ValueError:
            continue

    return now.isoformat()


class _SubListener:
    def __init__(self, emit_tick) -> None:
        self._emit_tick = emit_tick

    def onItemUpdate(self, update) -> None:  # noqa: N802
        item_name = update.getItemName()
        contract = update.getValue('Contract') or item_name
        instrument_id = update.getValue('InstrumentId') or ''

        price = _safe_float(update.getValue('Price'))
        if price is None:
            price = _safe_float(update.getValue('Last'))
        if price is None:
            price = _safe_float(update.getValue('Close'))
        if price is None:
            return

        bid = _safe_float(update.getValue('BestBid'))
        ask = _safe_float(update.getValue('BestAsk'))
        volume = _safe_float(update.getValue('Volume'))

        tick = {
            'timestamp': _parse_timestamp(update),
            'instrument': contract,
            'instrument_id': instrument_id,
            'item_name': item_name,
            'price': price,
            'bid': bid if bid is not None else price,
            'ask': ask if ask is not None else price,
            'volume': volume if volume is not None else 0.0,
        }
        self._emit_tick(tick)


class LightstreamerStreamThread(QThread):
    tick_received = pyqtSignal(dict)
    stream_status = pyqtSignal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._running = False
        self._ls_client = None
        self._subscription = None

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        self._running = True
        self.stream_status.emit('STARTED')

        if LightstreamerClient is None or Subscription is None:
            self.stream_status.emit('ERROR: lightstreamer-client package is not installed')
            self.stream_status.emit('STOPPED')
            return

        try:
            self._ls_client = LightstreamerClient(SERVER_URL, ADAPTER_SET)
            self._ls_client.connect()

            self._subscription = Subscription(
                'MERGE',
                ['TT-' + item for item in SUBSCRIBED_ITEMS],
                FIELD_NAMES,
            )
            self._subscription.setDataAdapter(DATA_ADAPTER)
            self._subscription.setRequestedMaxFrequency('0.5')
            self._subscription.setRequestedSnapshot('yes')
            self._subscription.addListener(_SubListener(self._emit_tick))

            self._ls_client.subscribe(self._subscription)
            while self._running:
                time.sleep(0.2)

        except Exception as exc:  # noqa: BLE001
            self.stream_status.emit(f'ERROR: {exc}')
        finally:
            self._cleanup()
            self.stream_status.emit('STOPPED')

    def _emit_tick(self, tick: dict) -> None:
        if self._running:
            self.tick_received.emit(tick)

    def _cleanup(self) -> None:
        try:
            if self._ls_client and self._subscription:
                self._ls_client.unsubscribe(self._subscription)
        except Exception:
            pass

        try:
            if self._ls_client:
                self._ls_client.disconnect()
        except Exception:
            pass

        self._ls_client = None
        self._subscription = None