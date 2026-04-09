from __future__ import annotations

import json
import re
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from PyQt6.QtCore import QByteArray, QSettings, Qt, QTimer
from PyQt6.QtWidgets import (
    QApplication,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from analytics.mean_reversion import estimate_half_life
from analytics.spread import build_fly, build_spread
from analytics.zscore import rolling_zscore
from charts.dashboard_widgets import ComparisonChartWidget, SignalLabWidget, ZScoreSnapshotWidget
from data.data_store import MarketDataStore
from data.historical_api import VALID_INTERVALS, datetime_to_unix_seconds, fetch_historical_ohlc
from data.lightstreamer_client import LightstreamerStreamThread, SUBSCRIBED_INSTRUMENTS
from ui.panels import DashboardControlPanel, StatsPanel


class HistoricalApiDialog(QDialog):
    def __init__(self, instruments: list[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Load Historical API Data")
        self.resize(420, 520)

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Instruments"))
        self.instrument_list = QListWidget()
        self.instrument_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        for instrument in instruments:
            item = QListWidgetItem(instrument)
            item.setSelected(True)
            self.instrument_list.addItem(item)
        layout.addWidget(self.instrument_list)

        layout.addWidget(QLabel("Interval"))
        self.interval_combo = QComboBox()
        self.interval_combo.setEditable(True)
        self.interval_combo.addItems(list(VALID_INTERVALS))
        self.interval_combo.setCurrentText("1D")
        layout.addWidget(self.interval_combo)

        layout.addWidget(QLabel("Count"))
        self.count_spin = QSpinBox()
        self.count_spin.setRange(0, 5000)
        self.count_spin.setSpecialValueText("")
        self.count_spin.setValue(0)
        layout.addWidget(self.count_spin)

        now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start_utc = datetime(2020, 1, 1, tzinfo=timezone.utc)

        layout.addWidget(QLabel("Start"))
        self.start_edit = QDateTimeEdit()
        self.start_edit.setCalendarPopup(True)
        self.start_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.start_edit.setDateTime(start_utc)
        layout.addWidget(self.start_edit)

        layout.addWidget(QLabel("End"))
        self.end_edit = QDateTimeEdit()
        self.end_edit.setCalendarPopup(True)
        self.end_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.end_edit.setDateTime(now_utc)
        layout.addWidget(self.end_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def payload(self) -> dict:
        instruments = [item.text() for item in self.instrument_list.selectedItems()]
        start_dt = self.start_edit.dateTime().toPyDateTime().replace(tzinfo=timezone.utc)
        end_dt = self.end_edit.dateTime().toPyDateTime().replace(tzinfo=timezone.utc)
        return {
            "instruments": instruments,
            "interval": self.interval_combo.currentText().strip().upper(),
            "count": int(self.count_spin.value()) or None,
            "start_unix": datetime_to_unix_seconds(start_dt),
            "end_unix": datetime_to_unix_seconds(end_dt),
        }


class MainWindow(QMainWindow):
    _EXCLUDED_INSTRUMENTS = {"H26"}
    _GENERIC_SPREAD_RANGES = {
        1: {
            "low": [-2.0, -0.75, -0.5, -0.25, -0.25, -0.25, -0.25, -0.25, -0.25, -0.25],
            "high": [0.25, 0.0, 0.5, 0.25, 0.25, 0.25, 0.5, 0.5, 0.75, 0.75],
        },
        2: {
            "low": [-3.0, -1.25, -1.0, -0.75, -0.25, -0.25, -0.25, 0.0, 0.0, 0.0],
            "high": [1.25, 1.0, 0.75, 0.75, 0.5, 0.75, 1.0, 1.0, 1.0, 1.25],
        },
    }
    _DARK_THEME = """
        QWidget {
            background: #101214;
            color: #ece7dc;
            font-family: "Segoe UI";
            font-size: __BASE_FONT__px;
        }
        QMainWindow, QSplitter, QTableWidget, QListWidget, QTextEdit, QAbstractItemView {
            background: #101214;
            color: #ece7dc;
        }
        QLabel#panelHeroTitle {
            font-size: __HERO_FONT__px;
            font-weight: 700;
            color: #f7f4ed;
        }
        QLabel#panelHeroCopy, QLabel#cardSubtitle, QLabel#cardMeta, QLabel#signalCopy {
            color: #9ea6ad;
        }
        QLabel#cardTitle {
            font-size: __CARD_TITLE_FONT__px;
            font-weight: 700;
            color: #f7f4ed;
        }
        QLabel#cardSubtitle, QLabel#cardMeta {
            font-size: __MICRO_FONT__px;
        }
        QLabel#statsHeadline, QLabel#signalHeadline {
            font-size: __SECTION_FONT__px;
            font-weight: 700;
            color: #f7f4ed;
        }
        QLabel#signalMetricName {
            color: #9ea6ad;
        }
        QLabel#signalMetricValue {
            color: #f7f4ed;
            font-weight: 600;
        }
        QTableWidget {
            gridline-color: #222931;
        }
        QTableWidget::item {
            padding: 4px 6px;
        }
        QWidget#dashboardCard, QFrame {
            background: #171a1d;
            border: 1px solid #2a3036;
            border-radius: 14px;
        }
        QGroupBox {
            border: 1px solid #2a3036;
            border-radius: 12px;
            margin-top: 12px;
            padding: 10px 10px 12px 10px;
            font-weight: 700;
            color: #f7f4ed;
        }
        QGroupBox::title {
            left: 10px;
            padding: 0 6px;
        }
        QPushButton, QComboBox, QSpinBox, QDateTimeEdit {
            background: #1d2329;
            border: 1px solid #303841;
            border-radius: 8px;
            padding: 6px 8px;
            color: #ece7dc;
        }
        QPushButton:hover, QComboBox:hover {
            background: #252d34;
        }
        QListWidget, QTableWidget, QTextEdit {
            background: #111418;
            border: 1px solid #2a3036;
            border-radius: 10px;
            alternate-background-color: #15191d;
        }
        QHeaderView::section {
            background: #1d2329;
            border: 0;
            border-bottom: 1px solid #303841;
            color: #d8d1c0;
            padding: 6px;
        }
    """

    _LIGHT_THEME = """
        QWidget {
            background: #f4efe4;
            color: #201b17;
            font-family: "Segoe UI";
            font-size: __BASE_FONT__px;
        }
        QWidget#dashboardCard, QFrame {
            background: #fffaf0;
            border: 1px solid #d8cdbd;
            border-radius: 14px;
        }
        QTableWidget {
            gridline-color: #e0d4c2;
        }
        QTableWidget::item {
            padding: 4px 6px;
        }
        QGroupBox {
            border: 1px solid #d8cdbd;
            border-radius: 12px;
            margin-top: 12px;
            padding: 10px 10px 12px 10px;
            font-weight: 700;
        }
        QGroupBox::title {
            left: 10px;
            padding: 0 6px;
        }
        QPushButton, QComboBox, QSpinBox, QDateTimeEdit, QListWidget, QTableWidget, QTextEdit {
            background: #fffaf0;
            border: 1px solid #d8cdbd;
            border-radius: 8px;
            padding: 6px 8px;
        }
        QHeaderView::section {
            background: #efe4d1;
            border: 0;
            border-bottom: 1px solid #d8cdbd;
            padding: 6px;
        }
    """

    def __init__(self, data_store: MarketDataStore, persist_path: Path) -> None:
        super().__init__()
        self.setWindowTitle("Rates Trading Workspace")
        self.resize(1860, 1080)

        self.data_store = data_store
        self.persist_path = persist_path
        self.settings = QSettings("RatesDashboard", "WorkspaceV2")
        self._syncing_controls = False
        self._last_instruments: list[str] = []
        self._last_history_dates: list[str] = []
        self._last_hover_payload: dict | None = None
        self._focus_mode = False
        self._tas_history: deque[tuple[str, str]] = deque(maxlen=40)
        self._history_refresh_inflight = False

        self.control_panel = DashboardControlPanel()
        self.stats_panel = StatsPanel()

        self.outright_chart = ComparisonChartWidget("Outrights", "")
        self.spread_3m_chart = ComparisonChartWidget("3M", "")
        self.spread_6m_chart = ComparisonChartWidget("6M", "")
        self.spread_9m_chart = ComparisonChartWidget("9M", "")
        self.zscore_chart = ZScoreSnapshotWidget("Z-Score", "")
        self.signal_lab = SignalLabWidget("Signals", "")

        for widget in [
            self.outright_chart,
            self.spread_3m_chart,
            self.spread_6m_chart,
            self.spread_9m_chart,
            self.zscore_chart,
        ]:
            widget.hover_changed.connect(self._on_chart_hovered)

        center_widget = QWidget()
        self.setCentralWidget(center_widget)
        root_layout = QVBoxLayout(center_widget)
        root_layout.setContentsMargins(10, 10, 10, 10)
        root_layout.setSpacing(10)
        self._center_widget = center_widget

        self.top_split = QSplitter(Qt.Orientation.Horizontal)
        root_layout.addWidget(self.top_split, 1)

        self.exit_focus_btn = QPushButton("Exit Focus", center_widget)
        self.exit_focus_btn.clicked.connect(self._toggle_focus_mode)
        self.exit_focus_btn.hide()
        self.exit_focus_btn.raise_()

        self.dashboard_split = QSplitter(Qt.Orientation.Vertical)
        self.dashboard_split.setChildrenCollapsible(False)

        self.row_one = QSplitter(Qt.Orientation.Horizontal)
        self.row_two = QSplitter(Qt.Orientation.Horizontal)
        self.row_three = QSplitter(Qt.Orientation.Horizontal)
        for row in [self.row_one, self.row_two, self.row_three]:
            row.setChildrenCollapsible(False)
            self.dashboard_split.addWidget(row)

        self.row_one.addWidget(self.outright_chart)
        self.row_one.addWidget(self.spread_3m_chart)
        self.row_two.addWidget(self.spread_6m_chart)
        self.row_two.addWidget(self.spread_9m_chart)
        self.row_three.addWidget(self.zscore_chart)
        self.row_three.addWidget(self.signal_lab)

        self.top_split.addWidget(self.control_panel)
        self.top_split.addWidget(self.dashboard_split)
        self.top_split.addWidget(self.stats_panel)
        self.top_split.setChildrenCollapsible(False)

        self.control_panel.config_changed.connect(self._on_controls_changed)
        self.control_panel.load_api_history_requested.connect(self._load_api_history)
        self.control_panel.theme_changed.connect(self._apply_theme)
        self.control_panel.reset_layout_requested.connect(self._reset_layout)
        self.control_panel.focus_mode_requested.connect(self._toggle_focus_mode)

        self._load_persisted_data()
        self._refresh_history_if_stale(force=False)
        self._load_settings()

        self.stream_thread = LightstreamerStreamThread()
        self.stream_thread.tick_received.connect(self.data_store.append_tick)
        self.stream_thread.tick_received.connect(self._on_live_tick)
        self.stream_thread.stream_status.connect(self._on_stream_status)
        self.stream_thread.start()

        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self._refresh_ui)
        self.ui_timer.start(300)

        self.persist_timer = QTimer(self)
        self.persist_timer.timeout.connect(self._persist_data)
        self.persist_timer.start(60000)

        self.history_refresh_timer = QTimer(self)
        self.history_refresh_timer.timeout.connect(lambda: self._refresh_history_if_stale(force=False))
        self.history_refresh_timer.start(60 * 60 * 1000)

        self._apply_theme(self.control_panel.config().get("theme", "dark"))
        self._reset_layout()
        self._refresh_ui()

    def _default_config(self) -> dict:
        return {
            "selected_instruments": list(SUBSCRIBED_INSTRUMENTS),
            "compare_date": None,
            "y_axis_mode": "actual",
            "live_price_mode": "last",
            "z_window": 14,
            "range_lookback": 30,
            "tas_threshold": 10000,
            "show_generic_ranges": True,
            "text_size": 12,
            "theme": "dark",
        }

    @staticmethod
    def _theme_metrics(text_size: int) -> dict[str, int]:
        base = int(text_size)
        return {
            "base_font": base,
            "hero_font": base + 10,
            "card_title_font": max(9, base - 1),
            "micro_font": max(8, base - 2),
            "section_font": base + 2,
        }

    def _apply_theme(self, theme_name: str) -> None:
        theme = str(theme_name).strip().lower()
        text_size = int(self.control_panel.config().get("text_size", 12))
        metrics = self._theme_metrics(text_size)
        stylesheet_template = self._LIGHT_THEME if theme == "light" else self._DARK_THEME
        stylesheet = (
            stylesheet_template
            .replace("__BASE_FONT__", str(metrics["base_font"]))
            .replace("__HERO_FONT__", str(metrics["hero_font"]))
            .replace("__CARD_TITLE_FONT__", str(metrics["card_title_font"]))
            .replace("__MICRO_FONT__", str(metrics["micro_font"]))
            .replace("__SECTION_FONT__", str(metrics["section_font"]))
        )
        app = QApplication.instance()
        if app is not None:
            app.setStyleSheet(stylesheet)
        for widget in [
            self.outright_chart,
            self.spread_3m_chart,
            self.spread_6m_chart,
            self.spread_9m_chart,
            self.zscore_chart,
            self.signal_lab,
        ]:
            widget.set_text_scale(text_size)

    def _reset_layout(self) -> None:
        self._focus_mode = False
        self.control_panel.show()
        self.stats_panel.show()
        self.control_panel.set_focus_mode(False)
        self.top_split.setSizes([250, 1460, 220])
        self.dashboard_split.setSizes([3, 3, 2])
        self.row_one.setSizes([1, 1])
        self.row_two.setSizes([1, 1])
        self.row_three.setSizes([1, 1])
        self.showNormal()

    def _toggle_focus_mode(self) -> None:
        self._focus_mode = not self._focus_mode
        self.control_panel.set_focus_mode(self._focus_mode)
        if self._focus_mode:
            self.control_panel.hide()
            self.stats_panel.hide()
            self.top_split.setSizes([0, 1900, 0])
            self.exit_focus_btn.show()
            self._position_exit_focus_button()
            self.showMaximized()
            return
        self.control_panel.show()
        self.stats_panel.show()
        self.exit_focus_btn.hide()
        self.top_split.setSizes([250, 1460, 220])
        self.showNormal()

    def _position_exit_focus_button(self) -> None:
        if not self.exit_focus_btn.isVisible():
            return
        margin = 18
        size = self.exit_focus_btn.sizeHint()
        self.exit_focus_btn.resize(size)
        x_pos = max(margin, self._center_widget.width() - size.width() - margin)
        self.exit_focus_btn.move(x_pos, margin)

    def _load_api_history(self) -> None:
        available_instruments = self._ordered_contracts(self.data_store.get_instruments())
        if not available_instruments:
            available_instruments = list(SUBSCRIBED_INSTRUMENTS)

        dialog = HistoricalApiDialog(available_instruments, self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        payload = dialog.payload()
        if not payload["instruments"]:
            QMessageBox.warning(self, "Historical API", "Select at least one instrument.")
            return
        if payload["start_unix"] >= payload["end_unix"]:
            QMessageBox.warning(self, "Historical API", "Start must be earlier than end.")
            return

        try:
            df = fetch_historical_ohlc(
                instruments=payload["instruments"],
                interval=payload["interval"],
                start_unix=payload["start_unix"],
                end_unix=payload["end_unix"],
                count=payload["count"],
            )
        except Exception as exc:  # noqa: BLE001
            QMessageBox.warning(self, "Historical API", str(exc))
            return

        if df.empty:
            QMessageBox.information(self, "Historical API", "No historical data was returned.")
            return

        self.data_store.load_historical(df)
        self._refresh_ui()
        QMessageBox.information(self, "Historical API", f"Loaded {len(df)} historical rows.")

    def _load_persisted_data(self) -> None:
        if not self.persist_path.exists():
            return
        try:
            df = pd.read_parquet(self.persist_path)
        except Exception:
            return
        if not df.empty:
            self.data_store.load_historical(df)

    def _refresh_history_if_stale(self, force: bool = False) -> None:
        if self._history_refresh_inflight:
            return
        instruments = self._ordered_contracts(list(SUBSCRIBED_INSTRUMENTS))
        if not instruments:
            return

        latest_history_ts = self.data_store.get_latest_history_timestamp()
        today_utc = datetime.now(timezone.utc).date()
        is_stale = latest_history_ts is None or latest_history_ts.date() < today_utc
        if not force and not is_stale:
            return

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=365 * 5)

        self._history_refresh_inflight = True
        try:
            try:
                df = fetch_historical_ohlc(
                    instruments=instruments,
                    interval="1D",
                    start_unix=datetime_to_unix_seconds(start_dt),
                    end_unix=datetime_to_unix_seconds(end_dt),
                    count=None,
                )
            except Exception as exc:  # noqa: BLE001
                self.stats_panel.update_status(f"Auto history load failed: {exc}")
                return

            if df.empty:
                self.stats_panel.update_status("Auto history load returned no rows")
                return

            self.data_store.load_historical(df)
            self.stats_panel.update_status(f"Auto history loaded ({len(df)} rows)")
        finally:
            self._history_refresh_inflight = False

    def _persist_data(self) -> None:
        self.data_store.persist_parquet(self.persist_path)

    def _save_settings(self) -> None:
        self.settings.setValue("geometry", bytes(self.saveGeometry().toBase64()).decode("ascii"))
        self.settings.setValue("window_state", bytes(self.saveState().toBase64()).decode("ascii"))
        self.settings.setValue("top_split_sizes", json.dumps(self.top_split.sizes()))
        self.settings.setValue("dashboard_split_sizes", json.dumps(self.dashboard_split.sizes()))
        self.settings.setValue("row_one_sizes", json.dumps(self.row_one.sizes()))
        self.settings.setValue("row_two_sizes", json.dumps(self.row_two.sizes()))
        self.settings.setValue("row_three_sizes", json.dumps(self.row_three.sizes()))
        self.settings.setValue("dashboard_config", json.dumps(self.control_panel.config()))

    def _load_settings(self) -> None:
        config = self._default_config()
        raw_config = self.settings.value("dashboard_config", "")
        if isinstance(raw_config, str) and raw_config:
            try:
                parsed = json.loads(raw_config)
                if isinstance(parsed, dict):
                    config.update(parsed)
            except Exception:
                pass

        self._syncing_controls = True
        try:
            self.control_panel.apply_config(config)
        finally:
            self._syncing_controls = False

        geometry = self.settings.value("geometry", "")
        if isinstance(geometry, str) and geometry:
            self.restoreGeometry(QByteArray.fromBase64(geometry.encode("ascii")))
        window_state = self.settings.value("window_state", "")
        if isinstance(window_state, str) and window_state:
            self.restoreState(QByteArray.fromBase64(window_state.encode("ascii")))

        for key, splitter in [
            ("top_split_sizes", self.top_split),
            ("dashboard_split_sizes", self.dashboard_split),
            ("row_one_sizes", self.row_one),
            ("row_two_sizes", self.row_two),
            ("row_three_sizes", self.row_three),
        ]:
            raw = self.settings.value(key, "")
            if not isinstance(raw, str) or not raw:
                continue
            try:
                sizes = json.loads(raw)
            except Exception:
                continue
            if isinstance(sizes, list) and sizes:
                splitter.setSizes([int(value) for value in sizes])

    def _on_controls_changed(self) -> None:
        if self._syncing_controls:
            return
        self._apply_theme(self.control_panel.config().get("theme", "dark"))
        self._refresh_ui()

    def _on_stream_status(self, status: str) -> None:
        self.stats_panel.update_status(status)

    def _on_live_tick(self, tick: dict) -> None:
        self.stats_panel.update_status("Live connected", str(tick.get("timestamp", "n/a")))

    def _on_chart_hovered(self, payload: dict) -> None:
        self._last_hover_payload = dict(payload)
        self.stats_panel.update_hover(
            payload.get("series_name"),
            payload.get("x_label"),
            payload.get("value"),
        )

    @classmethod
    def _contract_rank(cls, instrument: str) -> tuple[int, int]:
        match = re.search(r"([HMUZ])(\d{2})$", str(instrument).upper())
        if not match:
            return (9999, 99)
        month_code, year_text = match.groups()
        month_order = {"H": 1, "M": 2, "U": 3, "Z": 4}
        return (2000 + int(year_text), month_order.get(month_code, 99))

    @classmethod
    def _ordered_contracts(cls, instruments: list[str]) -> list[str]:
        unique = [
            str(item)
            for item in dict.fromkeys(str(item) for item in instruments if str(item).strip())
            if str(item) not in cls._EXCLUDED_INSTRUMENTS
        ]
        return sorted(unique, key=cls._contract_rank)

    def _selected_contracts(self) -> list[str]:
        selected = self.control_panel.selected_instruments()
        if selected:
            return self._ordered_contracts(selected)
        return self._ordered_contracts(self.data_store.get_instruments() or list(SUBSCRIBED_INSTRUMENTS))

    def _current_config(self) -> dict:
        cfg = self._default_config()
        cfg.update(self.control_panel.config())
        if not cfg.get("selected_instruments"):
            cfg["selected_instruments"] = self._selected_contracts()
        return cfg

    def _snapshot_for_date(self, pivot: pd.DataFrame, compare_date: str | None) -> pd.Series:
        if pivot.empty:
            return pd.Series(dtype=float)
        if not compare_date:
            return pivot.iloc[-1]
        target = pd.to_datetime(compare_date, utc=True, errors="coerce")
        if pd.isna(target):
            return pivot.iloc[-1]
        day_end = target.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        idx = pd.to_datetime(pivot.index, utc=True, errors="coerce")
        mask = idx <= day_end
        if not mask.any():
            return pd.Series(dtype=float)
        return pivot.loc[mask].iloc[-1]

    def _build_spread_series(self, snapshot: pd.Series, step: int) -> tuple[list[str], list[float]]:
        labels: list[str] = []
        values: list[float] = []
        if snapshot.empty:
            return labels, values
        ordered = [name for name in self._ordered_contracts(list(snapshot.index)) if name in snapshot.index]
        for idx in range(len(ordered) - step):
            left = ordered[idx]
            right = ordered[idx + step]
            left_value = pd.to_numeric(snapshot.get(left), errors="coerce")
            right_value = pd.to_numeric(snapshot.get(right), errors="coerce")
            if pd.isna(left_value) or pd.isna(right_value):
                continue
            labels.append(f"{left}/{right}")
            values.append(float(left_value) - float(right_value))
        return labels, values

    def _build_comparison_payload(
        self,
        current_snapshot: pd.Series,
        compare_snapshot: pd.Series,
        step: int,
    ) -> tuple[list[str], list[float], list[float]]:
        if step == 0:
            labels = [name for name in self._ordered_contracts(list(current_snapshot.index)) if name in current_snapshot.index]
            current_values = [float(pd.to_numeric(current_snapshot.get(label), errors="coerce")) for label in labels]
            compare_values = [float(pd.to_numeric(compare_snapshot.get(label), errors="coerce")) for label in labels]
            valid = [
                (label, current, compare)
                for label, current, compare in zip(labels, current_values, compare_values, strict=False)
                if pd.notna(current) and pd.notna(compare)
            ]
            return (
                [item[0] for item in valid],
                [float(item[1]) for item in valid],
                [float(item[2]) for item in valid],
            )

        current_labels, current_values = self._build_spread_series(current_snapshot, step)
        compare_labels, compare_values = self._build_spread_series(compare_snapshot, step)
        compare_map = dict(zip(compare_labels, compare_values, strict=False))
        labels = [label for label in current_labels if label in compare_map]
        return labels, [current_values[current_labels.index(label)] for label in labels], [compare_map[label] for label in labels]

    def _build_zscore_snapshot(self, pivot: pd.DataFrame, window: int) -> tuple[list[str], list[float], str]:
        labels: list[str] = []
        values: list[float] = []
        extreme_text = "Max |z|: n/a"
        if pivot.empty:
            return labels, values, extreme_text

        for label in self._ordered_contracts(list(pivot.columns)):
            series = pd.to_numeric(pivot[label], errors="coerce").dropna()
            if series.empty:
                continue
            z = rolling_zscore(series, window=window).dropna()
            if z.empty:
                continue
            labels.append(label)
            values.append(float(z.iloc[-1]))

        if labels:
            hottest_label, hottest_value = max(zip(labels, values, strict=False), key=lambda item: abs(item[1]))
            extreme_text = f"Max |z|: {hottest_label}  {hottest_value:.2f}"
        return labels, values, extreme_text

    def _generic_range_overlay(self, labels: list[str], step: int) -> tuple[list[float], list[float]]:
        config = self._GENERIC_SPREAD_RANGES.get(step)
        if not config or not labels:
            return [], []
        low_source = list(config["low"])
        high_source = list(config["high"])
        lows = [low_source[idx] if idx < len(low_source) else float("nan") for idx in range(len(labels))]
        highs = [high_source[idx] if idx < len(high_source) else float("nan") for idx in range(len(labels))]
        return lows, highs

    @staticmethod
    def _clamp01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _flow_maps(self, min_trade_qty: int) -> tuple[dict[str, float], dict[str, float]]:
        live_events = self.data_store.get_live_event_frame(max_age_seconds=20)
        if live_events.empty:
            return {}, {}
        tape = live_events.copy()
        tape["trade_qty"] = pd.to_numeric(tape.get("last_qty", 0.0), errors="coerce").fillna(0.0)
        tape = tape[tape["trade_qty"] >= float(max(1, min_trade_qty))]
        if tape.empty:
            return {}, {}
        tape["side"] = tape.apply(self._trade_side_from_row, axis=1)
        net_flow: dict[str, float] = {}
        gross_flow: dict[str, float] = {}
        for row in tape.itertuples(index=False):
            instrument = str(row.instrument)
            qty = float(row.trade_qty)
            sign = 1.0 if str(row.side).lower() == "bid" else -1.0
            net_flow[instrument] = net_flow.get(instrument, 0.0) + (sign * qty)
            gross_flow[instrument] = gross_flow.get(instrument, 0.0) + qty
        return net_flow, gross_flow

    def _execution_snapshot(self) -> tuple[dict[str, float], float]:
        latest = self.data_store.get_live_latest_snapshot()
        if latest.empty:
            return {}, 0.0
        latest["width"] = pd.to_numeric(latest["ask"], errors="coerce") - pd.to_numeric(latest["bid"], errors="coerce")
        latest = latest.dropna(subset=["instrument", "width"])
        if latest.empty:
            return {}, 0.0
        width_map = {str(row.instrument): max(0.0, float(row.width)) for row in latest.itertuples(index=False)}
        positive_widths = [value for value in width_map.values() if value > 0.0]
        baseline = float(np.median(positive_widths)) if positive_widths else 0.0
        return width_map, baseline

    @staticmethod
    def _regime_from_series(series: pd.Series, lookback: int) -> tuple[str, float]:
        returns = pd.to_numeric(series, errors="coerce").diff().dropna()
        if len(returns) < 20:
            return "Unknown", 0.45
        short_vol = float(returns.tail(min(10, len(returns))).std(ddof=0))
        long_vol = float(returns.tail(max(20, lookback)).std(ddof=0))
        if not np.isfinite(short_vol) or not np.isfinite(long_vol) or long_vol <= 0.0:
            return "Unknown", 0.45
        ratio = short_vol / long_vol
        if ratio <= 0.8:
            return "Low Vol", 0.9
        if ratio >= 1.2:
            return "High Vol", 0.35
        return "Balanced", 0.65

    @staticmethod
    def _half_life_score(half_life: float | None) -> tuple[str, float]:
        if half_life is None:
            return "n/a", 0.35
        if half_life <= 3:
            return f"{half_life:.1f}d", 0.95
        if half_life <= 7:
            return f"{half_life:.1f}d", 0.8
        if half_life <= 15:
            return f"{half_life:.1f}d", 0.6
        return f"{half_life:.1f}d", 0.3

    @staticmethod
    def _mean_reversion_profile(half_life: float | None) -> tuple[str, str, float]:
        if half_life is None:
            return "Unknown", "n/a", 0.35
        if half_life <= 2.5:
            return "Strong", "1-3d", 0.95
        if half_life <= 5.0:
            return "Good", "2-5d", 0.82
        if half_life <= 10.0:
            return "Moderate", "4-10d", 0.62
        if half_life <= 20.0:
            return "Slow", "1-3w", 0.38
        return "Weak", ">3w", 0.2

    @staticmethod
    def _confidence_band(score: float) -> str:
        if score >= 75:
            return "High"
        if score >= 55:
            return "Medium"
        return "Low"

    @staticmethod
    def _lagged_correlation(target: pd.Series, reference: pd.Series, lag: int) -> float | None:
        aligned = pd.concat(
            {
                "target": pd.to_numeric(target, errors="coerce"),
                "reference": pd.to_numeric(reference, errors="coerce").shift(lag),
            },
            axis=1,
        ).dropna()
        if len(aligned) < 20:
            return None
        corr = aligned["target"].corr(aligned["reference"])
        if pd.isna(corr):
            return None
        return float(corr)

    def _lead_lag_signal(
        self,
        label: str,
        structure_type: str,
        series_map: dict[str, pd.Series],
        z_value: float,
    ) -> tuple[str, float, str]:
        peers = [name for name in series_map.keys() if name != label]
        if not peers:
            return "None", 0.45, "No peer structures available."

        def shared_contracts(name_a: str, name_b: str) -> int:
            return len(set(name_a.split("/")) & set(name_b.split("/")))

        candidate_peers = [name for name in peers if shared_contracts(label, name) >= (2 if structure_type == "Fly" else 1)]
        if not candidate_peers:
            candidate_peers = peers

        target = pd.to_numeric(series_map[label], errors="coerce").dropna()
        if len(target) < 30:
            return "None", 0.45, "Not enough history for lag analysis."

        best_peer = None
        best_lag = None
        best_corr = None
        for peer in candidate_peers:
            ref = pd.to_numeric(series_map[peer], errors="coerce").dropna()
            if len(ref) < 30:
                continue
            for lag in range(1, 6):
                corr = self._lagged_correlation(target.diff(), ref.diff(), lag)
                if corr is None:
                    continue
                if best_corr is None or abs(corr) > abs(best_corr):
                    best_peer = peer
                    best_lag = lag
                    best_corr = corr

        if best_peer is None or best_lag is None or best_corr is None or abs(best_corr) < 0.2:
            return "Weak", 0.45, "No stable tenor lead detected."

        peer_series = pd.to_numeric(series_map[best_peer], errors="coerce").dropna()
        peer_move = float(peer_series.diff().tail(3).sum()) if len(peer_series) >= 4 else 0.0
        desired_move = -1.0 if z_value > 0 else 1.0
        aligned = desired_move * peer_move
        if aligned > 0:
            strength = 0.55 + min(0.35, abs(best_corr) * 0.35)
            label_text = f"{best_peer} leads {best_lag}d"
            detail = f"{best_peer} historically leads by {best_lag}d and is already moving in the reversion direction."
            return label_text, self._clamp01(strength), detail
        if aligned < 0:
            strength = 0.3
            label_text = f"{best_peer} contra {best_lag}d"
            detail = f"{best_peer} historically leads by {best_lag}d but recent move is against the reversion setup."
            return label_text, strength, detail
        label_text = f"{best_peer} lead {best_lag}d"
        detail = f"{best_peer} historically leads by {best_lag}d, but recent move is neutral."
        return label_text, 0.5, detail

    def _score_structure_flow(
        self,
        z_value: float,
        legs: list[str],
        weights: list[float],
        net_flow_map: dict[str, float],
        gross_flow_map: dict[str, float],
    ) -> tuple[str, float]:
        weighted_net = 0.0
        weighted_gross = 0.0
        for leg, weight in zip(legs, weights, strict=False):
            flow = float(net_flow_map.get(leg, 0.0))
            gross = float(gross_flow_map.get(leg, 0.0))
            weighted_net += weight * flow
            weighted_gross += abs(weight) * gross
        if weighted_gross <= 0.0:
            return "Neutral", 0.5
        support = (-1.0 if z_value > 0 else 1.0) * weighted_net / weighted_gross
        if support >= 0.25:
            return "Supportive", self._clamp01(0.5 + support)
        if support <= -0.25:
            return "Against", self._clamp01(0.5 + support)
        return "Mixed", self._clamp01(0.5 + support)

    def _execution_label(
        self,
        legs: list[str],
        width_map: dict[str, float],
        baseline_width: float,
    ) -> tuple[str, float]:
        if not legs:
            return "Unknown", 0.4
        widths = [float(width_map.get(leg, baseline_width)) for leg in legs if leg in width_map or baseline_width > 0.0]
        if not widths:
            return "Unknown", 0.4
        avg_width = float(np.mean(widths))
        if baseline_width <= 0.0:
            return "Fair", 0.55
        ratio = avg_width / baseline_width
        if ratio <= 1.0:
            return "Easy", 0.9
        if ratio <= 1.8:
            return "Tradable", 0.65
        return "Wide", 0.3

    def _trade_phrase(self, structure_type: str, label: str, z_value: float) -> str:
        side = self._trade_side(z_value)
        if "Spread" in structure_type:
            return f"{side} spread {label}"
        return f"{side} fly {label}"

    @staticmethod
    def _trade_side(z_value: float) -> str:
        return "Sell" if z_value > 0 else "Buy"

    def _evaluate_structure_candidate(
        self,
        label: str,
        structure_type: str,
        live_series: pd.Series,
        hist_series: pd.Series,
        series_map: dict[str, pd.Series],
        legs: list[str],
        weights: list[float],
        lookback: int,
        net_flow_map: dict[str, float],
        gross_flow_map: dict[str, float],
        width_map: dict[str, float],
        baseline_width: float,
    ) -> dict[str, object] | None:
        hist_clean = pd.to_numeric(hist_series, errors="coerce").dropna()
        live_clean = pd.to_numeric(live_series, errors="coerce").dropna()
        if len(hist_clean) < max(30, lookback):
            return None
        if live_clean.empty:
            return None

        analysis_series = live_clean.tail(max(lookback * 3, 90))
        current_value = float(live_clean.iloc[-1])
        z_window = min(max(20, lookback), len(analysis_series))
        z_series = rolling_zscore(analysis_series, window=z_window).dropna()
        if z_series.empty:
            return None
        z_value = float(z_series.iloc[-1])

        hist_window = hist_clean.tail(max(lookback, 30))
        low = float(hist_window.min())
        high = float(hist_window.max())
        width = high - low
        percentile = 50.0 if width <= 0.0 else float(((hist_window <= current_value).mean()) * 100.0)
        mean_window = analysis_series.tail(z_window)
        rolling_mean = float(pd.to_numeric(mean_window, errors="coerce").mean())
        rolling_std = float(pd.to_numeric(mean_window, errors="coerce").std(ddof=0))
        half_life = estimate_half_life(hist_clean.tail(max(lookback * 3, 120)))
        half_life_text, half_life_score = self._half_life_score(half_life)
        mr_strength, holding_text, mr_score = self._mean_reversion_profile(half_life)
        regime_text, regime_score = self._regime_from_series(hist_clean.tail(max(lookback * 2, 60)), lookback)
        leadlag_text, leadlag_score, leadlag_reason = self._lead_lag_signal(label, structure_type, series_map, z_value)
        flow_text, flow_score = self._score_structure_flow(z_value, legs, weights, net_flow_map, gross_flow_map)
        execution_text, execution_score = self._execution_label(legs, width_map, baseline_width)

        extreme_score = self._clamp01(abs(z_value) / 3.0)
        percentile_score = self._clamp01(abs(percentile - 50.0) / 50.0)
        confidence = round(
            100.0
            * (
                0.30 * extreme_score
                + 0.18 * percentile_score
                + 0.10 * half_life_score
                + 0.08 * mr_score
                + 0.14 * regime_score
                + 0.10 * leadlag_score
                + 0.10 * flow_score
                + 0.08 * execution_score
            )
        )
        trigger_value = None
        if np.isfinite(rolling_std) and rolling_std > 0.0:
            trigger_value = rolling_mean + (2.0 * rolling_std if z_value > 0 else -2.0 * rolling_std)
        side = self._trade_side(z_value)
        entry_text = "n/a"
        if trigger_value is not None:
            if (z_value > 0 and current_value >= trigger_value) or (z_value < 0 and current_value <= trigger_value):
                entry_text = "Live"
            else:
                entry_text = f"{trigger_value:.4f}"
        return {
            "trade": self._trade_phrase(structure_type, label, z_value),
            "side": side,
            "label": label,
            "type": structure_type,
            "current": current_value,
            "z_value": z_value,
            "zscore": f"{z_value:+.2f}",
            "percentile_value": percentile,
            "percentile": f"{percentile:.0f}%",
            "half_life_value": float(half_life) if half_life is not None else None,
            "half_life": half_life_text,
            "mr_strength": mr_strength,
            "holding": holding_text,
            "regime": regime_text,
            "leadlag": leadlag_text,
            "flow": flow_text,
            "execution": execution_text,
            "entry": entry_text,
            "confidence_value": confidence,
            "confidence": f"{confidence}%",
            "score": float(confidence),
            "reason": (
                f'{label} {structure_type.lower()} at {current_value:.4f}, z {z_value:+.2f}, percentile {percentile:.0f}%, '
                f'half-life {half_life_text}, MR {mr_strength.lower()}, expected hold {holding_text}, '
                f'good {side.lower()} trigger {entry_text}, '
                f'{regime_text.lower()} regime, lead-lag {leadlag_reason.lower()}, '
                f'flow {flow_text.lower()}, execution {execution_text.lower()}.'
            ),
        }

    def _build_lois_signal_payload(
        self,
        hist_pivot: pd.DataFrame,
        live_pivot: pd.DataFrame,
        contracts: list[str],
        cfg: dict,
    ) -> dict[str, object]:
        lookback = int(cfg.get("range_lookback", 30))
        tas_threshold = int(cfg.get("tas_threshold", 10000))
        signal_side = str(cfg.get("signal_side", "all")).strip().lower()
        if hist_pivot.empty or live_pivot.empty or len(contracts) < 3:
            return {
                "headline": "LOIS signal workbench waiting",
                "summary": "Need enough LOIS history plus a live snapshot before spreads and flies can be ranked.",
                "top_metric": "n/a",
                "regime_metric": "n/a",
                "leadlag_metric": "n/a",
                "execution_metric": "n/a",
                "rows": [],
                "notes_detail": "Waiting for historical API data and live flow.",
                "meta": f"{lookback}D lookback",
            }

        ordered = [name for name in self._ordered_contracts(contracts) if name in hist_pivot.columns and name in live_pivot.columns]
        net_flow_map, gross_flow_map = self._flow_maps(max(1, tas_threshold))
        width_map, baseline_width = self._execution_snapshot()
        candidates: list[dict[str, object]] = []
        spread_histories: dict[str, tuple[pd.Series, int]] = {}
        spread_lives: dict[str, tuple[pd.Series, int]] = {}
        fly_histories: dict[str, pd.Series] = {}
        fly_lives: dict[str, pd.Series] = {}

        for step in (1, 2, 3):
            for idx in range(len(ordered) - step):
                left = ordered[idx]
                right = ordered[idx + step]
                label = f"{left}/{right}"
                spread_histories[label] = (build_spread(hist_pivot, left, right), step)
                spread_lives[label] = (build_spread(live_pivot, left, right), step)

        for idx in range(len(ordered) - 2):
            left = ordered[idx]
            belly = ordered[idx + 1]
            right = ordered[idx + 2]
            label = f"{left}/{belly}/{right}"
            fly_histories[label] = build_fly(hist_pivot, left, belly, right)
            fly_lives[label] = build_fly(live_pivot, left, belly, right)

        for label, (hist_series, step) in spread_histories.items():
            left, right = label.split("/")
            live_series, _ = spread_lives[label]
            candidate = self._evaluate_structure_candidate(
                label,
                f"{step * 3}M Spread",
                live_series,
                hist_series,
                {name: series for name, (series, _step) in spread_histories.items()},
                [left, right],
                [1.0, -1.0],
                lookback,
                net_flow_map,
                gross_flow_map,
                width_map,
                baseline_width,
            )
            if candidate is not None:
                candidates.append(candidate)

        for idx in range(len(ordered) - 2):
            left = ordered[idx]
            belly = ordered[idx + 1]
            right = ordered[idx + 2]
            label = f"{left}/{belly}/{right}"
            hist_series = fly_histories[label]
            live_series = fly_lives[label]
            candidate = self._evaluate_structure_candidate(
                label,
                "Fly",
                live_series,
                hist_series,
                fly_histories,
                [left, belly, right],
                [1.0, -2.0, 1.0],
                lookback,
                net_flow_map,
                gross_flow_map,
                width_map,
                baseline_width,
            )
            if candidate is not None:
                candidates.append(candidate)

        if signal_side in {"buy", "sell"}:
            candidates = [item for item in candidates if str(item.get("side", "")).lower() == signal_side]

        if not candidates:
            return {
                "headline": "LOIS signal workbench warming up",
                "summary": "The strip is loaded, but there are no candidates matching the current history depth or side filter.",
                "top_metric": "n/a",
                "regime_metric": "n/a",
                "leadlag_metric": "n/a",
                "execution_metric": "n/a",
                "rows": [],
                "notes_detail": "Try Signal Side = All, or wait for a stronger dislocation / more history.",
                "meta": f"{lookback}D | side {signal_side}",
            }

        ranked = sorted(candidates, key=lambda item: float(item["score"]), reverse=True)
        top_rows = ranked[:8]
        top_pick = top_rows[0]
        rows = [
            {
                "trade": str(item["label"]),
                "side": str(item["side"]),
                "type": str(item["type"]),
                "zscore": str(item["zscore"]),
                "percentile": str(item["percentile"]),
                "half_life": str(item["half_life"]),
                "mr_strength": str(item["mr_strength"]),
                "regime": str(item["regime"]),
                "leadlag": str(item["leadlag"]),
                "flow": str(item["flow"]),
                "holding": str(item["holding"]),
                "entry": str(item["entry"]),
                "confidence": str(item["confidence"]),
            }
            for item in top_rows
        ]
        notes = []
        for idx, item in enumerate(top_rows[:3], start=1):
            direction_color = "#4aa3ff" if str(item.get("side", "")).lower() == "buy" else "#ff6b6b"
            notes.append(
                f"<div style='margin-bottom:12px; padding:10px 12px; border:1px solid #2a3036; border-radius:10px;'>"
                f"<div style='color:#9ea6ad; font-size:11px;'>Idea {idx}</div>"
                f"<div style='color:{direction_color}; font-weight:700; margin-top:2px;'>{item['trade']}</div>"
                f"<div style='margin-top:6px;'>"
                f"Side: {item['side']}<br/>"
                f"Structure: {item['label']} {item['type'].lower()}<br/>"
                f"Entry: {item['entry']}<br/>"
                f"Confidence: {item['confidence']} ({self._confidence_band(float(item['confidence_value']))})<br/>"
                f"Reason: {item['reason']}"
                f"</div></div>"
            )
        regime_counts = {
            "Low Vol": sum(1 for item in top_rows if item["regime"] == "Low Vol"),
            "Balanced": sum(1 for item in top_rows if item["regime"] == "Balanced"),
            "High Vol": sum(1 for item in top_rows if item["regime"] == "High Vol"),
        }
        supportive_flows = sum(1 for item in top_rows if item["flow"] == "Supportive")
        tradable_exec = sum(1 for item in top_rows if item["execution"] in {"Easy", "Tradable"})
        return {
            "headline": "LOIS spread / fly workbench live",
            "summary": (
                f"Ranking adjacent LOIS calendar spreads and flies using z-score, percentile, half-life, realized-vol regime, "
                f"live flow confirmation, lead-lag, execution quality, and trigger entry levels over a {lookback}D base window."
            ),
            "top_metric": f"{top_pick['label']} {top_pick['confidence']}",
            "regime_metric": (
                f"Low {regime_counts['Low Vol']} | Bal {regime_counts['Balanced']} | High {regime_counts['High Vol']}"
            ),
            "leadlag_metric": str(top_pick["leadlag"]),
            "execution_metric": f"{tradable_exec}/{len(top_rows)} tradable",
            "rows": rows,
            "notes_detail": "".join(notes),
            "meta": f"{lookback}D | flow>{tas_threshold:,} | side {signal_side} | {len(candidates)} candidates",
        }

    @staticmethod
    def _confidence_label(score: float) -> str:
        if score >= 0.84:
            return "High"
        if score >= 0.6:
            return "Medium"
        return "Low"

    def _build_range_signal(
        self,
        hist_pivot: pd.DataFrame,
        current_snapshot: pd.Series,
        lookback: int,
    ) -> tuple[str, str]:
        if hist_pivot.empty or current_snapshot.empty:
            return "Waiting", "Range engine needs historical data and a current snapshot."

        ordered = [name for name in self._ordered_contracts(list(hist_pivot.columns)) if name in current_snapshot.index]
        candidate_rows: list[dict[str, float | str]] = []
        window_set = sorted({int(lookback), max(30, int(lookback)), max(60, int(lookback) * 2)})

        def add_candidate(label: str, history: pd.Series, current_value: float) -> None:
            clean = pd.to_numeric(history, errors="coerce").dropna()
            if len(clean) < max(10, min(lookback, 15)):
                return
            windows_used: list[int] = []
            support_hits = 0
            base_mid_score = None
            base_pct = None
            base_low = None
            base_high = None
            for window in window_set:
                sample = clean.tail(window)
                if len(sample) < max(10, min(window, 15)):
                    continue
                low = float(sample.min())
                high = float(sample.max())
                width = high - low
                if width <= 0:
                    continue
                pct = (current_value - low) / width
                mid_score = max(0.0, 1.0 - min(1.0, abs(pct - 0.5) / 0.5))
                windows_used.append(window)
                support_hits += 1 if 0.2 <= pct <= 0.8 else 0
                if base_mid_score is None:
                    base_mid_score = mid_score
                    base_pct = pct
                    base_low = low
                    base_high = high
            if not windows_used or base_mid_score is None or base_pct is None or base_low is None or base_high is None:
                return
            confidence = support_hits / len(windows_used)
            score = (0.7 * float(base_mid_score)) + (0.3 * confidence)
            candidate_rows.append(
                {
                    "label": label,
                    "current": float(current_value),
                    "pct": float(base_pct),
                    "low": float(base_low),
                    "high": float(base_high),
                    "score": float(score),
                    "confidence": float(confidence),
                    "windows": "/".join(str(item) for item in windows_used),
                }
            )

        for label in ordered:
            current_value = pd.to_numeric(current_snapshot.get(label), errors="coerce")
            if pd.isna(current_value):
                continue
            add_candidate(label, hist_pivot[label], float(current_value))

        for step in (1, 2):
            for idx in range(len(ordered) - step):
                left = ordered[idx]
                right = ordered[idx + step]
                if left not in hist_pivot.columns or right not in hist_pivot.columns:
                    continue
                current_left = pd.to_numeric(current_snapshot.get(left), errors="coerce")
                current_right = pd.to_numeric(current_snapshot.get(right), errors="coerce")
                if pd.isna(current_left) or pd.isna(current_right):
                    continue
                spread_history = pd.to_numeric(hist_pivot[left], errors="coerce") - pd.to_numeric(
                    hist_pivot[right], errors="coerce"
                )
                add_candidate(f"{left}/{right}", spread_history, float(current_left) - float(current_right))

        if not candidate_rows:
            return "Insufficient", "Not enough historical depth yet to rank outright or spread ranges."

        ranked = sorted(candidate_rows, key=lambda item: item["score"], reverse=True)[:3]
        best = ranked[0]
        metric = f'{best["label"]} {self._confidence_label(float(best["confidence"]))}'
        detail_lines = []
        for item in ranked:
            pct = max(0.0, min(1.0, float(item["pct"]))) * 100.0
            detail_lines.append(
                f'{item["label"]} {float(item["current"]):.4f} in {lookback}D band {float(item["low"]):.4f}-{float(item["high"]):.4f} '
                f'({pct:.0f}th pct, conf {self._confidence_label(float(item["confidence"]))}, windows {item["windows"]}D)'
            )
        return metric, "\n".join(detail_lines)

    @staticmethod
    def _tas_side_html(side: str, instrument: str, price: float, qty: float) -> str:
        side_text = str(side).strip().title()
        color = "#4aa3ff" if side_text == "Bid" else "#ff6b6b"
        return f'{instrument} <span style="color:{color}; font-weight:600;">{side_text}</span> {price:.4f} x{int(qty):,}'

    @staticmethod
    def _trade_side_from_row(row: pd.Series) -> str:
        direction = str(row.get("direction", "") or "").strip().upper()
        if any(token in direction for token in ("SELL", "ASK", "OFFER", "HIT")):
            return "Ask"
        if any(token in direction for token in ("BUY", "BID", "LIFT")):
            return "Bid"
        bid_qty = pd.to_numeric(row.get("bid_qty"), errors="coerce")
        ask_qty = pd.to_numeric(row.get("ask_qty"), errors="coerce")
        bid_qty = 0.0 if pd.isna(bid_qty) else float(bid_qty)
        ask_qty = 0.0 if pd.isna(ask_qty) else float(ask_qty)
        return "Bid" if bid_qty >= ask_qty else "Ask"

    def _merge_tas_history(self, entries: list[tuple[str, str]]) -> str:
        for entry in entries:
            if any(existing[0] == entry[0] for existing in self._tas_history):
                continue
            self._tas_history.appendleft(entry)
        if not self._tas_history:
            return "TAS sync tape waiting for live quantity updates."
        return "<br/>".join(html for _, html in self._tas_history)

    @staticmethod
    def _ltp_change_html(delta: float) -> str:
        color = "#1ecb70" if delta > 0 else "#ff6b6b" if delta < 0 else "#9ea6ad"
        return f'<span style="color:{color}; font-weight:600;">{delta:+.4f}</span>'

    def _build_tas_flow_sections(self, tape: pd.DataFrame, tas_threshold: int) -> tuple[str, str]:
        flow = tape.copy()
        if flow.empty:
            return (
                f"<b>Aggression Flow</b><br/>No trade flow above {tas_threshold:,} yet.",
                "<b>LTP Moves</b><br/>No recent LTP changes.",
            )

        flow["side"] = flow.apply(self._trade_side_from_row, axis=1)
        flow["price"] = pd.to_numeric(flow["price"], errors="coerce")
        flow["trade_qty"] = pd.to_numeric(flow["trade_qty"], errors="coerce").fillna(0.0)
        flow = flow.dropna(subset=["price"])

        agg = (
            flow.groupby(["instrument", "side", "price"], as_index=False)["trade_qty"]
            .sum()
            .sort_values(["trade_qty", "instrument", "price"], ascending=[False, True, False])
        )
        agg = agg.head(10)
        flow_lines: list[str] = []
        for row in agg.itertuples(index=False):
            flow_lines.append(self._tas_side_html(str(row.side), str(row.instrument), float(row.price), float(row.trade_qty)))
        if not flow_lines:
            flow_lines.append(f"No trade flow above {tas_threshold:,} yet.")

        ltp = flow.sort_values(["instrument", "timestamp"])
        ltp_lines: list[str] = []
        for instrument in self._ordered_contracts(ltp["instrument"].dropna().astype(str).unique().tolist()):
            contract_rows = ltp[ltp["instrument"] == instrument]
            if contract_rows.empty:
                continue
            first_price = float(contract_rows.iloc[0]["price"])
            last_price = float(contract_rows.iloc[-1]["price"])
            delta = last_price - first_price
            total_qty = float(contract_rows["trade_qty"].sum())
            if abs(delta) < 1e-12 and total_qty < float(tas_threshold):
                continue
            ltp_lines.append(f'{instrument} {last_price:.4f} {self._ltp_change_html(delta)} x{int(total_qty):,}')
        if not ltp_lines:
            ltp_lines.append("No recent LTP changes.")

        aggression_html = "<b>Aggression Flow</b><br/>" + "<br/>".join(flow_lines)
        ltp_html = "<b>LTP Moves</b><br/>" + "<br/>".join(ltp_lines[:10])
        return aggression_html, ltp_html

    def _build_tas_signal(self, tas_threshold: int) -> tuple[str, str, str, str]:
        live_events = self.data_store.get_live_event_frame(max_age_seconds=12)
        if live_events.empty:
            return (
                "Tape idle",
                self._merge_tas_history([]),
                "<b>Aggression Flow</b><br/>No trade flow above threshold yet.",
                "<b>LTP Moves</b><br/>No recent LTP changes.",
            )

        tape = live_events.copy()
        tape["trade_qty"] = pd.to_numeric(tape.get("last_qty", 0.0), errors="coerce").fillna(0.0)
        qualifying_tape = tape[tape["trade_qty"] >= float(tas_threshold)].copy()
        if qualifying_tape.empty:
            aggression_html, ltp_html = self._build_tas_flow_sections(tape, tas_threshold)
            return f">{tas_threshold:,} none", self._merge_tas_history([]), aggression_html, ltp_html

        qualifying_tape["side"] = qualifying_tape.apply(self._trade_side_from_row, axis=1)
        qualifying_tape["bucket"] = pd.to_datetime(qualifying_tape["timestamp"], utc=True, errors="coerce").dt.floor("s")
        qualifying_tape["qty_bucket"] = (pd.to_numeric(qualifying_tape["trade_qty"], errors="coerce") / 1.0).round() * 1.0
        qualifying_tape = qualifying_tape.dropna(subset=["bucket"]).drop_duplicates(
            subset=["bucket", "qty_bucket", "instrument"], keep="last"
        )
        grouped = [
            group.assign(_rank=group["instrument"].map(lambda item: self._contract_rank(str(item))))
            .sort_values("_rank")
            .drop(columns=["_rank"])
            for _, group in qualifying_tape.groupby(["bucket", "qty_bucket"], sort=False)
            if group["instrument"].nunique() >= 2
        ]
        if not grouped:
            aggression_html, ltp_html = self._build_tas_flow_sections(tape, tas_threshold)
            return f">{tas_threshold:,} none", self._merge_tas_history([]), aggression_html, ltp_html

        grouped.sort(
            key=lambda group: (
                int(group["instrument"].nunique()),
                float(group["qty_bucket"].iloc[0]),
                pd.Timestamp(group["bucket"].iloc[0]).value,
            ),
            reverse=True,
        )
        top_groups = grouped[:2]
        first = top_groups[0]
        group_type = "Fly" if first["instrument"].nunique() >= 3 else "Spread"
        direction_type = "same-side" if first["side"].nunique() == 1 else "mixed-side"
        metric = f'{group_type} {direction_type}'
        history_entries: list[tuple[str, str]] = []
        for group in top_groups:
            legs = " | ".join(
                self._tas_side_html(str(row.side), str(row.instrument), float(row.price), float(row.trade_qty))
                for row in group.itertuples(index=False)
            )
            stamp = pd.Timestamp(group["bucket"].iloc[0]).strftime("%H:%M:%S")
            qty_text = int(float(group["qty_bucket"].iloc[0]))
            group_key = (
                f'{pd.Timestamp(group["bucket"].iloc[0]).isoformat()}|{qty_text}|'
                f'{"-".join(str(item) for item in group["instrument"].tolist())}'
            )
            history_entries.append((group_key, f"<b>{stamp}</b> qty {qty_text:,}: {legs}"))
        history_html = self._merge_tas_history(history_entries)
        aggression_html, ltp_html = self._build_tas_flow_sections(tape, tas_threshold)
        detail_html = f"<b>Sync Tape</b><br/>{history_html}"
        return metric, detail_html, aggression_html, ltp_html

    def _build_neighbor_signal(
        self,
        current_snapshot: pd.Series,
        compare_snapshot: pd.Series,
    ) -> tuple[str, str]:
        if current_snapshot.empty or compare_snapshot.empty:
            return "Waiting", "Neighbor divergence needs both current and comparison snapshots."

        ordered = [name for name in self._ordered_contracts(list(current_snapshot.index)) if name in compare_snapshot.index]
        candidates: list[dict[str, float | str]] = []
        for idx, label in enumerate(ordered):
            current_value = pd.to_numeric(current_snapshot.get(label), errors="coerce")
            compare_value = pd.to_numeric(compare_snapshot.get(label), errors="coerce")
            if pd.isna(current_value) or pd.isna(compare_value):
                continue
            neighbor_labels = []
            neighbor_changes = []
            for offset in (-1, 1):
                neighbor_idx = idx + offset
                if neighbor_idx < 0 or neighbor_idx >= len(ordered):
                    continue
                neighbor = ordered[neighbor_idx]
                n_current = pd.to_numeric(current_snapshot.get(neighbor), errors="coerce")
                n_compare = pd.to_numeric(compare_snapshot.get(neighbor), errors="coerce")
                if pd.isna(n_current) or pd.isna(n_compare):
                    continue
                neighbor_labels.append(neighbor)
                neighbor_changes.append(float(n_current) - float(n_compare))
            if not neighbor_changes:
                continue
            own_change = float(current_value) - float(compare_value)
            neighbor_avg = sum(neighbor_changes) / len(neighbor_changes)
            divergence = own_change - neighbor_avg
            candidates.append(
                {
                    "label": label,
                    "own_change": own_change,
                    "neighbor_avg": neighbor_avg,
                    "divergence": divergence,
                    "neighbors": "/".join(neighbor_labels),
                }
            )
        if not candidates:
            return "Insufficient", "Not enough neighboring outrights have both live and comparison levels."

        best = max(candidates, key=lambda item: abs(float(item["divergence"])))
        metric = f'{best["label"]} {float(best["divergence"]):+.4f}'
        detail = (
            f'{best["label"]} net change {float(best["own_change"]):+.4f} vs neighbors {best["neighbors"]} '
            f'avg {float(best["neighbor_avg"]):+.4f}, divergence {float(best["divergence"]):+.4f}.'
        )
        return metric, detail

    def _build_signal_payload(
        self,
        hist_pivot: pd.DataFrame,
        live_pivot: pd.DataFrame,
        current_snapshot: pd.Series,
        compare_snapshot: pd.Series,
        cfg: dict,
        compare_label: str,
    ) -> dict[str, str]:
        lookback = int(cfg.get("range_lookback", 30))
        tas_threshold = int(cfg.get("tas_threshold", 10000))
        range_metric, range_detail = self._build_range_signal(hist_pivot, current_snapshot, lookback)
        tas_metric, tas_detail, aggression_detail, ltp_detail = self._build_tas_signal(tas_threshold)
        neighbor_metric, neighbor_detail = self._build_neighbor_signal(current_snapshot, compare_snapshot)

        live_ok = not live_pivot.empty and self.data_store.get_live_snapshot_timestamp() is not None
        headline = "Signal stack live" if live_ok else "Signal stack on history only"
        summary = (
            f"Range analysis uses {lookback}D as the base window with longer-window confirmation. "
            f"TAS sync now uses live trade size from LastQty above {tas_threshold:,}, grouped by time and matching size."
        )
        bias_parts = [
            "Range"
            if range_metric not in {"Waiting", "Insufficient"}
            else "Flat",
            "TAS"
            if "none" not in tas_metric.lower() and "idle" not in tas_metric.lower()
            else "Quiet",
            "Neighbor"
            if neighbor_metric not in {"Waiting", "Insufficient"}
            else "Flat",
        ]
        return {
            "headline": headline,
            "summary": summary,
            "range_metric": range_metric,
            "tas_metric": tas_metric,
            "neighbor_metric": neighbor_metric,
            "bias_metric": " / ".join(bias_parts),
            "range_detail": range_detail,
            "tas_detail": tas_detail,
            "aggression_detail": aggression_detail,
            "ltp_detail": ltp_detail,
            "neighbor_detail": neighbor_detail,
            "meta": f"{compare_label} | {lookback}D | TAS>{tas_threshold:,}",
        }

    def _refresh_ui(self) -> None:
        instruments = self._ordered_contracts(self.data_store.get_instruments())
        if instruments != self._last_instruments:
            self._last_instruments = instruments
            current_config = self._current_config()
            self._syncing_controls = True
            try:
                self.control_panel.set_instruments(instruments or list(SUBSCRIBED_INSTRUMENTS))
                self.control_panel.apply_config(current_config)
            finally:
                self._syncing_controls = False

        history_dates = self.data_store.get_available_history_dates()
        if history_dates != self._last_history_dates:
            self._last_history_dates = history_dates
            current_config = self._current_config()
            self._syncing_controls = True
            try:
                self.control_panel.set_history_dates(history_dates)
                self.control_panel.apply_config(current_config)
            finally:
                self._syncing_controls = False

        cfg = self._current_config()
        contracts = self._selected_contracts()
        live_mode = str(cfg.get("live_price_mode", "last"))
        compare_date = cfg.get("compare_date")
        y_axis_mode = str(cfg.get("y_axis_mode", "actual"))
        show_generic_ranges = bool(cfg.get("show_generic_ranges", True))

        live_pivot = self.data_store.get_price_pivot(contracts, include_live=True, live_price_mode=live_mode)
        hist_pivot = self.data_store.get_price_pivot(contracts, include_live=False, live_price_mode=live_mode)

        current_snapshot = live_pivot.iloc[-1] if not live_pivot.empty else pd.Series(dtype=float)
        compare_snapshot = self._snapshot_for_date(hist_pivot, compare_date)
        if compare_snapshot.empty:
            compare_snapshot = hist_pivot.iloc[-1] if not hist_pivot.empty else current_snapshot.copy()

        labels, current_values, compare_values = self._build_comparison_payload(current_snapshot, compare_snapshot, 0)
        compare_label = compare_date or (self._last_history_dates[-1] if self._last_history_dates else "history")
        self.outright_chart.update_payload(labels, current_values, compare_values, y_axis_mode, compare_label)

        labels_3, current_3, compare_3 = self._build_comparison_payload(current_snapshot, compare_snapshot, 1)
        low_3, high_3 = self._generic_range_overlay(labels_3, 1)
        self.spread_3m_chart.update_payload(
            labels_3,
            current_3,
            compare_3,
            y_axis_mode,
            compare_label,
            low_3,
            high_3,
            show_generic_ranges,
        )

        labels_6, current_6, compare_6 = self._build_comparison_payload(current_snapshot, compare_snapshot, 2)
        low_6, high_6 = self._generic_range_overlay(labels_6, 2)
        self.spread_6m_chart.update_payload(
            labels_6,
            current_6,
            compare_6,
            y_axis_mode,
            compare_label,
            low_6,
            high_6,
            show_generic_ranges,
        )

        labels_9, current_9, compare_9 = self._build_comparison_payload(current_snapshot, compare_snapshot, 3)
        self.spread_9m_chart.update_payload(labels_9, current_9, compare_9, y_axis_mode, compare_label)

        z_labels, z_values, z_extreme_text = self._build_zscore_snapshot(live_pivot, int(cfg.get("z_window", 14)))
        self.zscore_chart.update_payload(z_labels, z_values, sigma_level=2.0)

        latest_table = self.data_store.get_latest_table(limit=40)

        signal_payload = self._build_lois_signal_payload(
            hist_pivot,
            live_pivot,
            contracts,
            cfg,
        )
        self.signal_lab.update_payload(signal_payload)

        summary_rows = [
            self._summary_row("Outrights", current_values, compare_values),
            self._summary_row("3M", current_3, compare_3),
            self._summary_row("6M", current_6, compare_6),
            self._summary_row("9M", current_9, compare_9),
        ]
        notes = (
            "The first four panels are now curve-comparison views. "
            "Use Compare Date to anchor the dashed reference line, and switch the axis between outright level and change."
        )
        self.stats_panel.update_context(compare_date, y_axis_mode, len(contracts), len(self.data_store.get_data()))
        self.stats_panel.update_summary_rows(summary_rows, z_extreme_text, notes)

        live_ts = self.data_store.get_live_snapshot_timestamp()
        if live_ts is not None:
            self.stats_panel.update_status("Live connected", str(live_ts))
        elif not latest_table.empty:
            self.stats_panel.update_status("Historical loaded")

        if self._last_hover_payload:
            self.stats_panel.update_hover(
                self._last_hover_payload.get("series_name"),
                self._last_hover_payload.get("x_label"),
                self._last_hover_payload.get("value"),
            )

    @staticmethod
    def _summary_row(name: str, current_values: list[float], compare_values: list[float]) -> dict[str, str]:
        if not current_values or not compare_values:
            return {"bucket": name, "latest": "n/a", "change": "n/a"}
        current_avg = sum(current_values) / len(current_values)
        compare_avg = sum(compare_values) / len(compare_values)
        return {
            "bucket": name,
            "latest": f"{current_avg:.4f}",
            "change": f"{(current_avg - compare_avg):+.4f}",
        }

    def closeEvent(self, event) -> None:  # noqa: N802
        self.ui_timer.stop()
        self.persist_timer.stop()
        self.history_refresh_timer.stop()
        self.stream_thread.stop()
        self.stream_thread.wait(2000)
        self._persist_data()
        self._save_settings()
        super().closeEvent(event)

    def resizeEvent(self, event) -> None:  # noqa: N802
        super().resizeEvent(event)
        self._position_exit_focus_button()
