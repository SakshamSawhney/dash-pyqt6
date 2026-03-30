from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
        self._autoload_history()
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

    def _autoload_history(self) -> None:
        if not self.data_store.get_data().empty:
            return

        instruments = self._ordered_contracts(list(SUBSCRIBED_INSTRUMENTS))
        if not instruments:
            return

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=365 * 5)

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
        self.spread_3m_chart.update_payload(labels_3, current_3, compare_3, y_axis_mode, compare_label)

        labels_6, current_6, compare_6 = self._build_comparison_payload(current_snapshot, compare_snapshot, 2)
        self.spread_6m_chart.update_payload(labels_6, current_6, compare_6, y_axis_mode, compare_label)

        labels_9, current_9, compare_9 = self._build_comparison_payload(current_snapshot, compare_snapshot, 3)
        self.spread_9m_chart.update_payload(labels_9, current_9, compare_9, y_axis_mode, compare_label)

        z_labels, z_values, z_extreme_text = self._build_zscore_snapshot(live_pivot, int(cfg.get("z_window", 14)))
        self.zscore_chart.update_payload(z_labels, z_values, sigma_level=2.0)

        latest_table = self.data_store.get_latest_table(limit=40)

        self.signal_lab.update_summary(compare_date, y_axis_mode, len(latest_table))

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
        self.stream_thread.stop()
        self.stream_thread.wait(2000)
        self._persist_data()
        self._save_settings()
        super().closeEvent(event)

    def resizeEvent(self, event) -> None:  # noqa: N802
        super().resizeEvent(event)
        self._position_exit_focus_button()
