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

SUBSCRIBED_CONTRACTS = [
    ('I MAR26 - ER3 MAR26 INTER-PRODUCT', '9134881030970970108'),
    ('I JUN26 - ER3 JUN26 INTER-PRODUCT', '14480190457373082110'),
    ('I SEP26 - ER3 SEP26 INTER-PRODUCT', '17906205016945586200'),
    ('I DEC26 - ER3 DEC26 INTER-PRODUCT', '8878079534340930517'),
    ('I MAR27 - ER3 MAR27 INTER-PRODUCT', '2759353391217821824'),
    ('I JUN27 - ER3 JUN27 INTER-PRODUCT', '7402492653967942221'),
    ('I SEP27 - ER3 SEP27 INTER-PRODUCT', '7776632799101019529'),
    ('I DEC27 - ER3 DEC27 INTER-PRODUCT', '13248090121204026347'),
    ('I MAR28 - ER3 MAR28 INTER-PRODUCT', '16373948182659265494'),
    ('I JUN28 - ER3 JUN28 INTER-PRODUCT', '13628906360113030952'),
    ('I SEP28 - ER3 SEP28 INTER-PRODUCT', '8313082975694645104'),
    ('I DEC28 - ER3 DEC28 INTER-PRODUCT', '13966546979499997890'),
    ('I MAR29 - ER3 MAR29 INTER-PRODUCT', '13632071925106489259'),
    ('I JUN29 - ER3 JUN29 INTER-PRODUCT', '6172152353938190254'),
    ('I SEP29 - ER3 SEP29 INTER-PRODUCT', '1521672764543319076'),
    ('I DEC29 - ER3 DEC29 INTER-PRODUCT', '14742885477615197897'),
]
SUBSCRIBED_CODES = [
    'H26', 'M26', 'U26', 'Z26',
    'H27', 'M27', 'U27', 'Z27',
    'H28', 'M28', 'U28', 'Z28',
    'H29', 'M29', 'U29', 'Z29',
]
SUBSCRIBED_ITEMS = [contract_id for _, contract_id in SUBSCRIBED_CONTRACTS]
SUBSCRIBED_INSTRUMENTS = list(SUBSCRIBED_CODES)
CONTRACT_ID_TO_INSTRUMENT = {
    contract_id: code for (contract_name, contract_id), code in zip(SUBSCRIBED_CONTRACTS, SUBSCRIBED_CODES, strict=True)
}
CONTRACT_NAME_TO_INSTRUMENT = {
    contract_name: code for (contract_name, _), code in zip(SUBSCRIBED_CONTRACTS, SUBSCRIBED_CODES, strict=True)
}


def _safe_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    try:
        if isinstance(value, str):
            text = value.strip().replace(',', '')
            if text in ('', '-', '--', 'N/A', 'n/a', 'null', 'None'):
                return None
            return float(text)
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_contract_id(value: Any) -> str:
    text = str(value or '').strip()
    if text.startswith('TT-'):
        text = text[3:]
    if text.endswith('.0'):
        text = text[:-2]
    return text


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
        self._last_price_by_instrument: dict[str, float] = {}

    def onItemUpdate(self, update) -> None:  # noqa: N802
        item_name = update.getItemName()
        instrument_id = update.getValue('InstrumentId') or ''
        contract = ''
        for candidate in (item_name, instrument_id):
            contract_key = _normalize_contract_id(candidate)
            if contract_key in CONTRACT_ID_TO_INSTRUMENT:
                contract = CONTRACT_ID_TO_INSTRUMENT[contract_key]
                break
        if not contract:
            raw_contract = str(update.getValue('Contract') or item_name)
            contract = CONTRACT_NAME_TO_INSTRUMENT.get(raw_contract, raw_contract)

        bid = _safe_float(update.getValue('BestBid'))
        ask = _safe_float(update.getValue('BestAsk'))
        bid_qty = _safe_float(update.getValue('BestBidQty'))
        ask_qty = _safe_float(update.getValue('BestAskQty'))
        volume = _safe_float(update.getValue('Volume'))

        price_candidates = [
            _safe_float(update.getValue('Price')),
            _safe_float(update.getValue('Last')),
            _safe_float(update.getValue('Close')),
            _safe_float(update.getValue('Settle')),
            _safe_float(update.getValue('PrevSettle')),
            _safe_float(update.getValue('IndSettle')),
            _safe_float(update.getValue('AdminPrice')),
            _safe_float(update.getValue('Open')),
            _safe_float(update.getValue('High')),
            _safe_float(update.getValue('Low')),
        ]
        price = next((p for p in price_candidates if p is not None), None)
        if price is None and bid is not None and ask is not None:
            price = (bid + ask) / 2.0
        if price is None:
            price = bid if bid is not None else ask
        if price is None and contract in self._last_price_by_instrument:
            price = self._last_price_by_instrument[contract]
        if price is None:
            return
        self._last_price_by_instrument[contract] = float(price)

        tick = {
            'timestamp': _parse_timestamp(update),
            'instrument': contract,
            'instrument_id': instrument_id,
            'item_name': item_name,
            'price': price,
            'bid': bid if bid is not None else price,
            'ask': ask if ask is not None else price,
            'bid_qty': bid_qty if bid_qty is not None else 0.0,
            'ask_qty': ask_qty if ask_qty is not None else 0.0,
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
        self._listener = None

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
            self._listener = _SubListener(self._emit_tick)
            self._subscription.addListener(self._listener)

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
        self._listener = None
