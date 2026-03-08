from __future__ import annotations

import json
import threading
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import QSettings, Qt, QThread, QTimer, pyqtSignal
from PyQt6.QtWidgets import (
    QHBoxLayout,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from analytics.correlation import correlation_matrix
from analytics.curve import build_curve_points
from analytics.regression import run_ols_regression
from analytics.spread import build_fly, build_spread
from analytics.zscore import rolling_zscore
from charts.curve_chart import CurveChartWidget
from charts.realtime_chart import RealtimeChartWidget
from charts.zscore_chart import ZScoreChartWidget
from data.data_store import MarketDataStore
from data.lightstreamer_client import LightstreamerStreamThread
from ui.panels import BottomTablePanel, LeftControlPanel, StatsPanel
from utils.math_utils import safe_last


class AnalyticsEngineThread(QThread):
    analytics_ready = pyqtSignal(dict)

    def __init__(self, data_store: MarketDataStore, parent=None) -> None:
        super().__init__(parent)
        self._data_store = data_store
        self._running = False
        self._config_lock = threading.Lock()
        self._config = {
            'selected_instruments': [],
            'spreads': [],
            'flies': [],
            'yield_curve_instruments': [],
            'z_window': 200,
            'sigma_level': 2.0,
        }

    def update_config(self, config: dict) -> None:
        with self._config_lock:
            self._config = dict(config)

    def stop(self) -> None:
        self._running = False

    @staticmethod
    def _primary_series(pivot: pd.DataFrame, cfg: dict) -> pd.Series:
        spreads = cfg.get('spreads', [])
        flies = cfg.get('flies', [])
        selected = cfg.get('selected_instruments', [])

        for entry in spreads:
            if not isinstance(entry, dict) or not entry.get('enabled', True):
                continue
            legs = entry.get('legs', [])
            if isinstance(legs, list) and len(legs) == 2:
                s = build_spread(pivot, str(legs[0]), str(legs[1]))
                if not s.empty:
                    return s

        for entry in flies:
            if not isinstance(entry, dict) or not entry.get('enabled', True):
                continue
            legs = entry.get('legs', [])
            if isinstance(legs, list) and len(legs) == 3:
                s = build_fly(pivot, str(legs[0]), str(legs[1]), str(legs[2]))
                if not s.empty:
                    return s

        for ins in selected:
            if ins in pivot.columns:
                return pivot[ins]

        return pd.Series(dtype=float)

    def run(self) -> None:
        self._running = True
        while self._running:
            with self._config_lock:
                cfg = dict(self._config)

            selected = cfg.get('selected_instruments', [])
            curve_instruments = cfg.get('yield_curve_instruments', [])
            z_window = int(cfg.get('z_window', 200))

            pivot = self._data_store.get_price_pivot(selected if selected else None)
            primary_series = self._primary_series(pivot, cfg) if not pivot.empty else pd.Series(dtype=float)

            zscore_last = None
            if not primary_series.empty:
                zscore_last = safe_last(rolling_zscore(primary_series, window=z_window).to_numpy())

            corr = correlation_matrix(pivot, selected)
            corr_text = corr.round(4).to_string() if not corr.empty else 'Not enough data/instruments.'

            regression_text = 'Not enough data for regression.'
            if not primary_series.empty:
                lois_cols = [c for c in pivot.columns if c.upper().startswith('LOIS')]
                y_series = pivot[lois_cols[0]] if lois_cols else pd.Series(dtype=float)
                if not y_series.empty:
                    reg = run_ols_regression(y_series=y_series, x_series=primary_series)
                    if reg['beta'] is not None:
                        regression_text = (
                            f"beta: {reg['beta']:.5f}\n"
                            f"r_squared: {reg['r_squared']:.5f}\n"
                            f"p_value: {reg['p_value']:.5f}"
                        )

            curve_df = pd.DataFrame()
            curve_pivot = self._data_store.get_price_pivot(curve_instruments if curve_instruments else None)
            if not curve_pivot.empty:
                curve_df = build_curve_points(curve_pivot.iloc[-1])

            self.analytics_ready.emit(
                {
                    'zscore': zscore_last,
                    'correlation_text': corr_text,
                    'regression_text': regression_text,
                    'curve_df': curve_df,
                }
            )
            self.msleep(500)


class ChartView(QWidget):
    def __init__(self, name: str, chart_type: str, config: dict | None = None) -> None:
        super().__init__()
        self.name = name
        self.chart_type = chart_type
        self.config = config or {
            'selected_instruments': [],
            'spreads': [],
            'flies': [],
            'yield_curve_instruments': [],
            'z_window': 200,
            'sigma_level': 2.0,
        }

        layout = QVBoxLayout(self)
        self.realtime_chart = None
        self.zscore_chart = None
        self.curve_chart = None

        if chart_type == 'market':
            self.realtime_chart = RealtimeChartWidget(f'{name} - Market')
            layout.addWidget(self.realtime_chart)
        elif chart_type == 'zscore':
            self.zscore_chart = ZScoreChartWidget(f'{name} - Z-Score')
            layout.addWidget(self.zscore_chart)
        elif chart_type == 'yield':
            self.curve_chart = CurveChartWidget(f'{name} - Yield Curve')
            layout.addWidget(self.curve_chart)


class MainWindow(QMainWindow):
    def __init__(self, data_store: MarketDataStore, persist_path: Path) -> None:
        super().__init__()
        self.setWindowTitle('Rates Trading Dashboard')
        self.resize(1700, 980)

        self.data_store = data_store
        self.persist_path = persist_path
        self._last_instruments: list[str] = []
        self._loading_panel = False
        self.analytics_thread = None

        self.settings = QSettings('RatesDashboard', 'Workspace')

        self.left_panel = LeftControlPanel()
        self.stats_panel = StatsPanel()
        self.table_panel = BottomTablePanel()

        self.chart_tabs = QTabWidget()
        self.chart_tabs.currentChanged.connect(self._on_tab_changed)

        add_market_btn = QPushButton('Add Market Chart')
        add_zscore_btn = QPushButton('Add Z-Score Chart')
        add_yield_btn = QPushButton('Add Yield Chart')
        remove_chart_btn = QPushButton('Remove Chart')
        add_market_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='market'))
        add_zscore_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='zscore'))
        add_yield_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='yield'))
        remove_chart_btn.clicked.connect(self._remove_current_chart_tab)

        center_widget = QWidget()
        self.setCentralWidget(center_widget)
        root_layout = QVBoxLayout(center_widget)

        top_split = QSplitter()

        center_container = QWidget()
        center_layout = QVBoxLayout(center_container)
        controls_row = QHBoxLayout()
        controls_row.addWidget(add_market_btn)
        controls_row.addWidget(add_zscore_btn)
        controls_row.addWidget(add_yield_btn)
        controls_row.addWidget(remove_chart_btn)
        controls_row.addStretch(1)
        center_layout.addLayout(controls_row)
        center_layout.addWidget(self.chart_tabs)

        top_split.addWidget(self.left_panel)
        top_split.addWidget(center_container)
        top_split.addWidget(self.stats_panel)
        top_split.setStretchFactor(0, 2)
        top_split.setStretchFactor(1, 7)
        top_split.setStretchFactor(2, 2)

        vertical_split = QSplitter()
        vertical_split.setOrientation(Qt.Orientation.Vertical)
        vertical_split.addWidget(top_split)
        vertical_split.addWidget(self.table_panel)
        vertical_split.setStretchFactor(0, 8)
        vertical_split.setStretchFactor(1, 2)

        root_layout.addWidget(vertical_split)

        self.left_panel.config_changed.connect(self._on_config_changed)

        self._load_workspace()

        self.stream_thread = LightstreamerStreamThread()
        self.stream_thread.tick_received.connect(self.data_store.append_tick)
        self.stream_thread.tick_received.connect(self._on_live_tick)
        self.stream_thread.stream_status.connect(self._on_stream_status)
        self.stream_thread.start()

        self.analytics_thread = AnalyticsEngineThread(self.data_store)
        self.analytics_thread.analytics_ready.connect(self._on_analytics_ready)
        self.analytics_thread.start()
        self._sync_panel_from_active_tab()
        self._on_config_changed()

        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self._refresh_ui)
        self.ui_timer.start(200)

        self.persist_timer = QTimer(self)
        self.persist_timer.timeout.connect(self._persist_data)
        self.persist_timer.start(60000)

    def _default_chart_config(self) -> dict:
        return {
            'selected_instruments': [],
            'spreads': [],
            'flies': [],
            'yield_curve_instruments': [],
            'z_window': 200,
            'sigma_level': 2.0,
        }

    def _current_chart_view(self) -> ChartView | None:
        widget = self.chart_tabs.currentWidget()
        if widget is None:
            return None
        return widget  # type: ignore[return-value]

    def _add_chart_tab(self, chart_type: str, config: dict | None = None, name: str | None = None) -> None:
        chart_label = {'market': 'Market', 'zscore': 'ZScore', 'yield': 'Yield'}.get(chart_type, 'Chart')
        tab_name = name or f'{chart_label} {self.chart_tabs.count() + 1}'
        view = ChartView(tab_name, chart_type, config or self._default_chart_config())
        self.chart_tabs.addTab(view, tab_name)
        self.chart_tabs.setCurrentWidget(view)

    def _remove_current_chart_tab(self) -> None:
        if self.chart_tabs.count() <= 1:
            QMessageBox.information(self, 'Chart Tabs', 'At least one chart tab must remain.')
            return

        idx = self.chart_tabs.currentIndex()
        widget = self.chart_tabs.widget(idx)
        self.chart_tabs.removeTab(idx)
        if widget:
            widget.deleteLater()

    def _sync_panel_from_active_tab(self) -> None:
        chart = self._current_chart_view()
        if chart is None:
            return

        self._loading_panel = True
        try:
            if self._last_instruments:
                self.left_panel.set_instruments(self._last_instruments)
            self.left_panel.set_config(chart.config)
        finally:
            self._loading_panel = False

    def _on_tab_changed(self, _index: int) -> None:
        self._sync_panel_from_active_tab()
        if getattr(self, 'analytics_thread', None) is None:
            return
        self._on_config_changed()

    def _on_config_changed(self) -> None:
        if self._loading_panel:
            return

        chart = self._current_chart_view()
        if chart is None:
            return

        chart.config = self.left_panel.get_config()
        analytics_thread = getattr(self, 'analytics_thread', None)
        if analytics_thread is not None:
            analytics_thread.update_config(chart.config)

    def _on_stream_status(self, status: str) -> None:
        self.stats_panel.update_live(status=status)

    def _on_live_tick(self, tick: dict) -> None:
        ts = str(tick.get('timestamp', 'n/a'))
        self.stats_panel.update_live(status='connected', last_tick=ts)

    def _on_analytics_ready(self, payload: dict) -> None:
        self.stats_panel.update_stats(
            zscore=payload.get('zscore'),
            corr_text=payload.get('correlation_text', ''),
            regression_text=payload.get('regression_text', ''),
        )

        chart = self._current_chart_view()
        if chart is not None and chart.chart_type == 'yield' and chart.curve_chart is not None:
            chart.curve_chart.update_curve(payload.get('curve_df', pd.DataFrame()))

    @staticmethod
    def _build_active_series(pivot: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, list[str]]:
        active_series = list(cfg.get('selected_instruments', []))

        for entry in cfg.get('spreads', []):
            if not isinstance(entry, dict) or not entry.get('enabled', True):
                continue
            legs = entry.get('legs', [])
            if not isinstance(legs, list) or len(legs) != 2:
                continue
            c1, c2 = str(legs[0]), str(legs[1])
            s = build_spread(pivot, c1, c2)
            if s.empty:
                continue
            label = f'{c1}-{c2}'
            pivot[label] = s
            active_series.append(label)

        for entry in cfg.get('flies', []):
            if not isinstance(entry, dict) or not entry.get('enabled', True):
                continue
            legs = entry.get('legs', [])
            if not isinstance(legs, list) or len(legs) != 3:
                continue
            c1, c2, c3 = str(legs[0]), str(legs[1]), str(legs[2])
            s = build_fly(pivot, c1, c2, c3)
            if s.empty:
                continue
            label = f'{c1}-2*{c2}+{c3}'
            pivot[label] = s
            active_series.append(label)

        return pivot, active_series

    @staticmethod
    def _primary_series_from_cfg(pivot: pd.DataFrame, cfg: dict) -> pd.Series:
        enriched, _ = MainWindow._build_active_series(pivot.copy(), cfg)

        for entry in cfg.get('spreads', []):
            if isinstance(entry, dict) and entry.get('enabled', True):
                legs = entry.get('legs', [])
                if isinstance(legs, list) and len(legs) == 2:
                    label = f'{legs[0]}-{legs[1]}'
                    if label in enriched.columns:
                        return enriched[label]

        for entry in cfg.get('flies', []):
            if isinstance(entry, dict) and entry.get('enabled', True):
                legs = entry.get('legs', [])
                if isinstance(legs, list) and len(legs) == 3:
                    label = f'{legs[0]}-2*{legs[1]}+{legs[2]}'
                    if label in enriched.columns:
                        return enriched[label]

        for ins in cfg.get('selected_instruments', []):
            if ins in enriched.columns:
                return enriched[ins]

        return pd.Series(dtype=float)

    def _refresh_ui(self) -> None:
        instruments = self.data_store.get_instruments()
        if instruments and instruments != self._last_instruments:
            self._last_instruments = instruments
            self._loading_panel = True
            try:
                self.left_panel.set_instruments(instruments)
                active = self._current_chart_view()
                if active:
                    self.left_panel.set_config(active.config)
            finally:
                self._loading_panel = False

        for idx in range(self.chart_tabs.count()):
            chart: ChartView = self.chart_tabs.widget(idx)  # type: ignore[assignment]
            cfg = chart.config

            if chart.chart_type == 'market' and chart.realtime_chart is not None:
                selected = cfg.get('selected_instruments', [])
                pivot = self.data_store.get_price_pivot(selected if selected else None)
                if pivot.empty:
                    chart.realtime_chart.clear_missing(set())
                    continue
                pivot, active_series = self._build_active_series(pivot, cfg)
                chart.realtime_chart.update_from_pivot(pivot, active_series)
                chart.realtime_chart.clear_missing(set(active_series))

            elif chart.chart_type == 'zscore' and chart.zscore_chart is not None:
                selected = cfg.get('selected_instruments', [])
                pivot = self.data_store.get_price_pivot(selected if selected else None)
                if pivot.empty:
                    chart.zscore_chart.update_series_map({}, sigma_level=float(cfg.get('sigma_level', 2.0)))
                    continue

                pivot_enriched, active_series = self._build_active_series(pivot.copy(), cfg)
                z_window = int(cfg.get('z_window', 200))
                sigma_level = float(cfg.get('sigma_level', 2.0))

                z_map: dict[str, pd.Series] = {}
                for name in active_series:
                    if name not in pivot_enriched.columns:
                        continue
                    z = rolling_zscore(pivot_enriched[name], window=z_window)
                    if z.notna().any():
                        z_map[name] = z

                chart.zscore_chart.update_series_map(z_map, sigma_level=sigma_level)

            elif chart.chart_type == 'yield' and chart.curve_chart is not None:
                curve_ins = cfg.get('yield_curve_instruments', [])
                curve_pivot = self.data_store.get_price_pivot(curve_ins if curve_ins else None)
                if curve_pivot.empty:
                    chart.curve_chart.update_curve(pd.DataFrame())
                    continue
                curve_df = build_curve_points(curve_pivot.iloc[-1])
                chart.curve_chart.update_curve(curve_df)

        latest = self.data_store.get_latest_table(limit=50)
        if not latest.empty:
            rows = latest.tail(30).astype(str).values.tolist()
            self.table_panel.update_rows(rows)

    def _persist_data(self) -> None:
        self.data_store.persist_parquet(self.persist_path)

    def _save_workspace(self) -> None:
        charts = []
        for idx in range(self.chart_tabs.count()):
            chart: ChartView = self.chart_tabs.widget(idx)  # type: ignore[assignment]
            charts.append({'name': chart.name, 'chart_type': chart.chart_type, 'config': chart.config})

        self.settings.setValue('charts', json.dumps(charts))
        self.settings.setValue('active_tab', self.chart_tabs.currentIndex())

    def _load_workspace(self) -> None:
        raw = self.settings.value('charts', '')
        loaded = False
        if isinstance(raw, str) and raw:
            try:
                charts = json.loads(raw)
                if isinstance(charts, list) and charts:
                    for entry in charts:
                        name = str(entry.get('name', f'Chart {self.chart_tabs.count() + 1}'))
                        chart_type = str(entry.get('chart_type', 'market'))
                        config = entry.get('config', self._default_chart_config())
                        self._add_chart_tab(chart_type=chart_type, config=config, name=name)
                    loaded = True
            except Exception:
                loaded = False

        if not loaded:
            self._add_chart_tab(chart_type='market', config=self._default_chart_config(), name='Market 1')

        idx = int(self.settings.value('active_tab', 0))
        if 0 <= idx < self.chart_tabs.count():
            self.chart_tabs.setCurrentIndex(idx)

    def closeEvent(self, event) -> None:  # noqa: N802
        self.ui_timer.stop()
        self.persist_timer.stop()

        if self.analytics_thread is not None:
            self.analytics_thread.stop()
            self.analytics_thread.wait(2000)

        self.stream_thread.stop()
        self.stream_thread.wait(2000)

        self._persist_data()
        self._save_workspace()
        super().closeEvent(event)