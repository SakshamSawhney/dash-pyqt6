from __future__ import annotations

import json
import threading
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import QByteArray, QSettings, Qt, QThread, QTimer, pyqtSignal
from PyQt6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabBar,
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
from data.lightstreamer_client import LightstreamerStreamThread, SUBSCRIBED_INSTRUMENTS
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
            'club_members': [],
            'yield_compare_dates': [],
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
            if curve_pivot.empty and curve_instruments:
                curve_pivot = self._data_store.get_price_pivot(None)
            if not curve_pivot.empty:
                curve_cfg = dict(cfg)
                curve_cfg['selected_instruments'] = list(curve_instruments)
                curve_enriched, curve_active = MainWindow._build_active_series(curve_pivot.copy(), curve_cfg)
                active_cols = [name for name in curve_active if name in curve_enriched.columns]
                curve_snapshot = curve_enriched[active_cols].iloc[-1] if active_cols else curve_pivot.iloc[-1]
                curve_df = build_curve_points(curve_snapshot)

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
    selected = pyqtSignal()

    def __init__(self, name: str, chart_type: str, config: dict | None = None) -> None:
        super().__init__()
        self.name = name
        self.chart_type = chart_type
        self.config = config or {
            'selected_instruments': [],
            'spreads': [],
            'flies': [],
            'yield_curve_instruments': [],
            'club_members': [],
            'yield_compare_dates': [],
            'z_window': 200,
            'sigma_level': 2.0,
        }

        layout = QVBoxLayout(self)
        self.realtime_chart = None
        self.zscore_chart = None
        self.curve_chart = None
        self.placeholder_label = None
        self._pending_view_state = None

        if chart_type == 'market':
            self.realtime_chart = RealtimeChartWidget(f'{name} - Market')
            layout.addWidget(self.realtime_chart)
        elif chart_type == 'zscore':
            self.zscore_chart = ZScoreChartWidget(f'{name} - Z-Score')
            layout.addWidget(self.zscore_chart)
        elif chart_type == 'yield':
            self.curve_chart = CurveChartWidget(f'{name} - Yield Curve')
            layout.addWidget(self.curve_chart)
        elif chart_type == 'club':
            self.placeholder_label = QLabel('Select charts in the left panel to show them in this club.')
            self.placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(self.placeholder_label)
        self.set_active(False)

    def export_state(self) -> dict:
        state = {'name': self.name, 'chart_type': self.chart_type, 'config': self.config}
        if self.realtime_chart is not None:
            state['view_state'] = self.realtime_chart.export_view_state()
        elif self.zscore_chart is not None:
            state['view_state'] = self.zscore_chart.export_view_state()
        elif self.curve_chart is not None:
            state['view_state'] = self.curve_chart.export_view_state()
        return state

    def restore_view_state(self) -> None:
        if not isinstance(self._pending_view_state, dict):
            return
        if self.realtime_chart is not None:
            self.realtime_chart.restore_view_state(self._pending_view_state)
        elif self.zscore_chart is not None:
            self.zscore_chart.restore_view_state(self._pending_view_state)
        elif self.curve_chart is not None:
            self.curve_chart.restore_view_state(self._pending_view_state)

    def set_active(self, active: bool) -> None:
        border = '#1e88e5' if active else '#808080'
        width = 2 if active else 1
        self.setStyleSheet(f'ChartView {{ border: {width}px solid {border}; border-radius: 4px; }}')

    def mousePressEvent(self, event) -> None:  # noqa: N802
        self.selected.emit()
        super().mousePressEvent(event)


class MainWindow(QMainWindow):
    def __init__(self, data_store: MarketDataStore, persist_path: Path) -> None:
        super().__init__()
        self.setWindowTitle('Rates Trading Dashboard')
        self.resize(1700, 980)

        self.data_store = data_store
        self.persist_path = persist_path
        self._last_instruments: list[str] = []
        self._last_history_dates: list[str] = []
        self._preferred_instruments: list[str] = list(SUBSCRIBED_INSTRUMENTS)
        self._loading_panel = False
        self.analytics_thread = None
        self._left_panel_width = 320
        self._right_panel_width = 320

        self.settings = QSettings('RatesDashboard', 'Workspace')

        self.left_panel = LeftControlPanel()
        self.stats_panel = StatsPanel()
        self.table_panel = BottomTablePanel()

        self.chart_views: list[ChartView] = []
        self.chart_tabs = QTabBar()
        self.chart_tabs.setMovable(True)
        self.chart_tabs.currentChanged.connect(self._on_tab_changed)
        self.chart_tabs.tabBarDoubleClicked.connect(self._rename_chart_tab)
        self.chart_tabs.tabMoved.connect(self._on_tab_moved)

        add_market_btn = QPushButton('Add Market Chart')
        add_zscore_btn = QPushButton('Add Z-Score Chart')
        add_yield_btn = QPushButton('Add Yield Chart')
        add_club_btn = QPushButton('Add Club Chart')
        remove_chart_btn = QPushButton('Remove Chart')
        self.toggle_left_btn = QPushButton('Hide Left Panel')
        self.toggle_right_btn = QPushButton('Hide Right Panel')
        self.use_live_mid_price_chk = QCheckBox('Use VWAP(Bid/Ask Qty)')
        save_workspace_btn = QPushButton('Save Workspace')
        load_workspace_btn = QPushButton('Load Workspace')
        add_market_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='market'))
        add_zscore_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='zscore'))
        add_yield_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='yield'))
        add_club_btn.clicked.connect(lambda: self._add_chart_tab(chart_type='club'))
        remove_chart_btn.clicked.connect(self._remove_current_chart_tab)
        self.toggle_left_btn.clicked.connect(self._toggle_left_panel)
        self.toggle_right_btn.clicked.connect(self._toggle_right_panel)
        self.use_live_mid_price_chk.toggled.connect(self._on_global_mid_price_toggled)
        save_workspace_btn.clicked.connect(self._save_workspace_and_notify)
        load_workspace_btn.clicked.connect(self._reload_workspace_and_notify)

        center_widget = QWidget()
        self.setCentralWidget(center_widget)
        root_layout = QVBoxLayout(center_widget)

        top_split = QSplitter()
        self.top_split = top_split

        center_container = QWidget()
        center_layout = QVBoxLayout(center_container)
        controls_row = QHBoxLayout()
        controls_row.addWidget(add_market_btn)
        controls_row.addWidget(add_zscore_btn)
        controls_row.addWidget(add_yield_btn)
        controls_row.addWidget(add_club_btn)
        controls_row.addWidget(remove_chart_btn)
        controls_row.addWidget(self.toggle_left_btn)
        controls_row.addWidget(self.toggle_right_btn)
        controls_row.addWidget(self.use_live_mid_price_chk)
        controls_row.addWidget(save_workspace_btn)
        controls_row.addWidget(load_workspace_btn)
        controls_row.addStretch(1)
        center_layout.addLayout(controls_row)
        center_layout.addWidget(self.chart_tabs)
        self.chart_splitter = QSplitter(Qt.Orientation.Vertical)
        center_layout.addWidget(self.chart_splitter)

        top_split.addWidget(self.left_panel)
        top_split.addWidget(center_container)
        top_split.addWidget(self.stats_panel)
        top_split.setChildrenCollapsible(False)
        top_split.setStretchFactor(0, 2)
        top_split.setStretchFactor(1, 7)
        top_split.setStretchFactor(2, 2)

        vertical_split = QSplitter()
        self.vertical_split = vertical_split
        vertical_split.setOrientation(Qt.Orientation.Vertical)
        vertical_split.addWidget(top_split)
        vertical_split.addWidget(self.table_panel)
        vertical_split.setStretchFactor(0, 8)
        vertical_split.setStretchFactor(1, 2)

        root_layout.addWidget(vertical_split)

        self.left_panel.config_changed.connect(self._on_config_changed)

        self._load_workspace()
        self._last_instruments = self._ordered_instruments([])
        if self._last_instruments:
            self._loading_panel = True
            try:
                self.left_panel.set_instruments(self._last_instruments)
            finally:
                self._loading_panel = False

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
            'club_members': [],
            'yield_compare_dates': [],
            'z_window': 200,
            'sigma_level': 2.0,
        }

    def _current_chart_view(self) -> ChartView | None:
        idx = self.chart_tabs.currentIndex()
        if idx < 0 or idx >= len(self.chart_views):
            return None
        return self.chart_views[idx]

    def _add_chart_tab(self, chart_type: str, config: dict | None = None, name: str | None = None) -> None:
        chart_label = {'market': 'Market', 'zscore': 'ZScore', 'yield': 'Yield', 'club': 'Club'}.get(chart_type, 'Chart')
        tab_name = name or f'{chart_label} {self.chart_tabs.count() + 1}'
        view = ChartView(tab_name, chart_type, config or self._default_chart_config())
        view.selected.connect(lambda v=view: self._on_chart_clicked(v))
        self.chart_views.append(view)
        self.chart_tabs.addTab(tab_name)
        self.chart_tabs.setCurrentIndex(len(self.chart_views) - 1)
        self._render_chart_views()

    def _remove_current_chart_tab(self) -> None:
        if self.chart_tabs.count() <= 1:
            QMessageBox.information(self, 'Chart Tabs', 'At least one chart tab must remain.')
            return

        idx = self.chart_tabs.currentIndex()
        if idx < 0 or idx >= len(self.chart_views):
            return
        widget = self.chart_views.pop(idx)
        self._remove_chart_from_clubs(widget.name)
        self.chart_tabs.removeTab(idx)
        widget.deleteLater()
        next_idx = min(idx, self.chart_tabs.count() - 1)
        if next_idx >= 0:
            self.chart_tabs.setCurrentIndex(next_idx)
        self._render_chart_views()

    def _rename_chart_tab(self, index: int) -> None:
        if index < 0 or index >= self.chart_tabs.count():
            return

        chart = self.chart_views[index]

        current_name = self.chart_tabs.tabText(index).strip() or chart.name
        new_name, accepted = QInputDialog.getText(
            self,
            'Rename Chart Tab',
            'Chart name',
            text=current_name,
        )
        if not accepted:
            return

        new_name = new_name.strip()
        if not new_name or new_name == current_name:
            return

        self._rename_chart_in_clubs(current_name, new_name)
        chart.name = new_name
        self.chart_tabs.setTabText(index, new_name)
        self._sync_panel_from_active_tab()
        self._render_chart_views()

    def _on_tab_moved(self, from_index: int, to_index: int) -> None:
        if from_index == to_index:
            return
        chart = self.chart_views.pop(from_index)
        self.chart_views.insert(to_index, chart)
        self._render_chart_views()

    def _on_chart_clicked(self, chart: ChartView) -> None:
        try:
            idx = self.chart_views.index(chart)
        except ValueError:
            return
        self.chart_tabs.setCurrentIndex(idx)

    def _rename_chart_in_clubs(self, old_name: str, new_name: str) -> None:
        for chart in self.chart_views:
            members = [str(name) for name in chart.config.get('club_members', [])]
            if not members:
                continue
            updated = [new_name if name == old_name else name for name in members]
            chart.config['club_members'] = updated

    def _remove_chart_from_clubs(self, chart_name: str) -> None:
        for chart in self.chart_views:
            members = [str(name) for name in chart.config.get('club_members', [])]
            if not members:
                continue
            chart.config['club_members'] = [name for name in members if name != chart_name]

    def _displayed_chart_views(self) -> list[ChartView]:
        active = self._current_chart_view()
        if active is None:
            return []
        if active.chart_type == 'club':
            selected_names = [str(name) for name in active.config.get('club_members', [])]
            name_to_chart = {
                chart.name: chart
                for chart in self.chart_views
                if chart is not active and chart.chart_type != 'club'
            }
            members = [name_to_chart[name] for name in selected_names if name in name_to_chart]
            if members:
                return members
        return [active]

    def _render_chart_views(self) -> None:
        self._clear_chart_splitter()

        displayed = self._displayed_chart_views()
        active = self._current_chart_view()
        for chart in self.chart_views:
            chart.set_active(False)
        if not displayed:
            return

        if len(displayed) == 1:
            chart = displayed[0]
            chart.set_active(chart is active)
            self.chart_splitter.addWidget(chart)
            self.chart_splitter.setSizes([1])
            return

        row_splitters: list[QSplitter] = []
        for idx in range(0, len(displayed), 2):
            row_splitter = QSplitter(Qt.Orientation.Horizontal)
            row_splitters.append(row_splitter)
            for chart in displayed[idx : idx + 2]:
                chart.set_active(chart is active)
                row_splitter.addWidget(chart)
            row_splitter.setChildrenCollapsible(False)
            row_splitter.setSizes([1] * row_splitter.count())
            self.chart_splitter.addWidget(row_splitter)

        self.chart_splitter.setChildrenCollapsible(False)
        self.chart_splitter.setSizes([1] * len(row_splitters))

    def _clear_chart_splitter(self) -> None:
        while self.chart_splitter.count():
            widget = self.chart_splitter.widget(0)
            self.chart_splitter.widget(0).setParent(None)
            if isinstance(widget, QSplitter):
                while widget.count():
                    child = widget.widget(0)
                    widget.widget(0).setParent(None)

    def _sync_panel_from_active_tab(self) -> None:
        chart = self._current_chart_view()
        if chart is None:
            return

        self._loading_panel = True
        try:
            if self._last_instruments:
                self.left_panel.set_instruments(self._last_instruments)
            available_chart_names = [
                view.name
                for view in self.chart_views
                if view is not chart and view.chart_type != 'club'
            ]
            self.left_panel.set_available_charts(available_chart_names, current_name=chart.name)
            self.left_panel.set_chart_type(chart.chart_type)
            self.left_panel.set_config(chart.config)
        finally:
            self._loading_panel = False

    def _on_tab_changed(self, _index: int) -> None:
        self._render_chart_views()
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

    def _on_global_mid_price_toggled(self, enabled: bool) -> None:
        self.data_store.set_use_live_mid_price(bool(enabled))

    def _toggle_left_panel(self) -> None:
        sizes = self.top_split.sizes()
        if self.left_panel.isVisible():
            self._left_panel_width = max(0, sizes[0]) if sizes else self._left_panel_width
            self.left_panel.hide()
            center = max(600, (sizes[1] if len(sizes) > 1 else 900) + self._left_panel_width)
            right = sizes[2] if len(sizes) > 2 else self._right_panel_width
            self.top_split.setSizes([0, center, right])
            self.toggle_left_btn.setText('Show Left Panel')
            return

        self.left_panel.show()
        sizes = self.top_split.sizes()
        center = max(600, sizes[1] if len(sizes) > 1 else 900)
        right = sizes[2] if len(sizes) > 2 else self._right_panel_width
        self.top_split.setSizes([self._left_panel_width or 320, center, right])
        self.toggle_left_btn.setText('Hide Left Panel')

    def _toggle_right_panel(self) -> None:
        sizes = self.top_split.sizes()
        if self.stats_panel.isVisible():
            self._right_panel_width = max(0, sizes[2]) if len(sizes) > 2 else self._right_panel_width
            self.stats_panel.hide()
            left = sizes[0] if sizes else self._left_panel_width
            center = max(600, (sizes[1] if len(sizes) > 1 else 900) + self._right_panel_width)
            self.top_split.setSizes([left, center, 0])
            self.toggle_right_btn.setText('Show Right Panel')
            return

        self.stats_panel.show()
        sizes = self.top_split.sizes()
        left = sizes[0] if sizes else self._left_panel_width
        center = max(600, sizes[1] if len(sizes) > 1 else 900)
        self.top_split.setSizes([left, center, self._right_panel_width or 320])
        self.toggle_right_btn.setText('Hide Right Panel')

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
        live_ts = self.data_store.get_live_snapshot_timestamp()
        if live_ts is not None:
            self.stats_panel.update_live(status='connected', last_tick=str(live_ts))

        history_dates = self.data_store.get_available_history_dates()
        if history_dates != self._last_history_dates:
            self._last_history_dates = history_dates
            self._loading_panel = True
            try:
                self.left_panel.set_yield_available_dates(history_dates)
                active = self._current_chart_view()
                if active:
                    self.left_panel.set_config(active.config)
            finally:
                self._loading_panel = False

        instruments = self._ordered_instruments(self.data_store.get_instruments())
        if instruments != self._last_instruments:
            self._last_instruments = instruments
            self._loading_panel = True
            try:
                self.left_panel.set_instruments(instruments)
                active = self._current_chart_view()
                if active:
                    self.left_panel.set_config(active.config)
            finally:
                self._loading_panel = False

        for chart in self.chart_views:
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
                curves_map = self._build_yield_curves_map(cfg)
                chart.curve_chart.update_curves_map(curves_map)

            chart.restore_view_state()
            chart._pending_view_state = None

        latest = self.data_store.get_latest_table(limit=50)
        if not latest.empty:
            rows = latest.tail(30).astype(str).values.tolist()
            self.table_panel.update_rows(rows)

    def _ordered_instruments(self, observed: list[str]) -> list[str]:
        preferred = [str(x) for x in self._preferred_instruments]
        ordered = preferred.copy()
        preferred_set = set(preferred)
        extras = [name for name in observed if name not in preferred_set]
        return ordered + extras

    def _build_yield_curves_map(self, cfg: dict) -> dict[str, pd.DataFrame]:
        curve_ins = list(cfg.get('yield_curve_instruments', []))
        compare_dates = [str(x) for x in cfg.get('yield_compare_dates', [])]
        curve_cfg = dict(cfg)
        curve_cfg['selected_instruments'] = list(curve_ins)

        curves: dict[str, pd.DataFrame] = {}

        live_pivot = self.data_store.get_price_pivot(curve_ins if curve_ins else None, include_live=True)
        if live_pivot.empty and curve_ins:
            live_pivot = self.data_store.get_price_pivot(None, include_live=True)
        if not live_pivot.empty:
            live_enriched, live_active = self._build_active_series(live_pivot.copy(), curve_cfg)
            live_cols = [name for name in live_active if name in live_enriched.columns]
            live_snapshot = live_enriched[live_cols].iloc[-1] if live_cols else live_pivot.iloc[-1]
            live_curve = build_curve_points(live_snapshot)
            if not live_curve.empty:
                curves['Live'] = live_curve

        if not compare_dates:
            return curves

        hist_pivot = self.data_store.get_price_pivot(curve_ins if curve_ins else None, include_live=False)
        if hist_pivot.empty and curve_ins:
            hist_pivot = self.data_store.get_price_pivot(None, include_live=False)
        if hist_pivot.empty:
            return curves

        hist_enriched, hist_active = self._build_active_series(hist_pivot.copy(), curve_cfg)
        hist_cols = [name for name in hist_active if name in hist_enriched.columns]
        snapshot_cols = hist_cols if hist_cols else list(hist_pivot.columns)
        if not snapshot_cols:
            return curves

        idx_utc = pd.to_datetime(hist_enriched.index, utc=True, errors='coerce')
        for date_text in compare_dates:
            target = pd.to_datetime(date_text, utc=True, errors='coerce')
            if pd.isna(target):
                continue
            day_start = target.normalize()
            day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            mask = idx_utc <= day_end
            if not mask.any():
                continue
            snapshot = hist_enriched.loc[mask, snapshot_cols].iloc[-1]
            curve_df = build_curve_points(snapshot)
            if not curve_df.empty:
                curves[date_text] = curve_df

        return curves

    def _persist_data(self) -> None:
        self.data_store.persist_parquet(self.persist_path)

    def _serialize_workspace(self) -> dict:
        active = self._current_chart_view()
        if active is not None and not self._loading_panel:
            active.config = self.left_panel.get_config()

        charts = []
        for chart in self.chart_views:
            charts.append(chart.export_state())

        return {
            'version': 2,
            'charts': charts,
            'active_tab': self.chart_tabs.currentIndex(),
            'global_settings': {
                'use_live_mid_price': self.use_live_mid_price_chk.isChecked(),
            },
            'geometry': bytes(self.saveGeometry().toBase64()).decode('ascii'),
            'window_state': bytes(self.saveState().toBase64()).decode('ascii'),
            'top_split_sizes': self.top_split.sizes(),
            'vertical_split_sizes': self.vertical_split.sizes(),
        }

    def _save_workspace(self) -> None:
        self.settings.setValue('workspace_state', json.dumps(self._serialize_workspace()))

    def _clear_chart_tabs(self) -> None:
        while self.chart_tabs.count():
            self.chart_tabs.removeTab(0)
        self._clear_chart_splitter()
        while self.chart_views:
            self.chart_views.pop().deleteLater()

    def _apply_workspace_payload(self, payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False

        charts = payload.get('charts', [])
        if not isinstance(charts, list) or not charts:
            return False

        self._loading_panel = True
        try:
            self._clear_chart_tabs()
            for entry in charts:
                if not isinstance(entry, dict):
                    continue
                name = str(entry.get('name', f'Chart {self.chart_tabs.count() + 1}'))
                chart_type = str(entry.get('chart_type', 'market'))
                config = entry.get('config', self._default_chart_config())
                self._add_chart_tab(chart_type=chart_type, config=config, name=name)
                chart = self._current_chart_view()
                if chart is not None:
                    chart._pending_view_state = entry.get('view_state')

            if self.chart_tabs.count() == 0:
                self._add_chart_tab(chart_type='market', config=self._default_chart_config(), name='Market 1')

            idx = int(payload.get('active_tab', 0))
            if 0 <= idx < self.chart_tabs.count():
                self.chart_tabs.setCurrentIndex(idx)

            global_settings = payload.get('global_settings', {})
            if isinstance(global_settings, dict):
                self.use_live_mid_price_chk.setChecked(bool(global_settings.get('use_live_mid_price', False)))
            elif charts:
                legacy_mid_price = any(
                    isinstance(entry, dict)
                    and bool(entry.get('config', {}).get('use_live_mid_price', False))
                    for entry in charts
                )
                self.use_live_mid_price_chk.setChecked(legacy_mid_price)

            geometry = payload.get('geometry')
            if isinstance(geometry, str) and geometry:
                self.restoreGeometry(QByteArray.fromBase64(geometry.encode('ascii')))

            window_state = payload.get('window_state')
            if isinstance(window_state, str) and window_state:
                self.restoreState(QByteArray.fromBase64(window_state.encode('ascii')))

            top_sizes = payload.get('top_split_sizes')
            if isinstance(top_sizes, list) and top_sizes:
                self.top_split.setSizes([int(x) for x in top_sizes])

            vertical_sizes = payload.get('vertical_split_sizes')
            if isinstance(vertical_sizes, list) and vertical_sizes:
                self.vertical_split.setSizes([int(x) for x in vertical_sizes])
        finally:
            self._loading_panel = False

        self._sync_panel_from_active_tab()
        self._render_chart_views()
        analytics_thread = getattr(self, 'analytics_thread', None)
        if analytics_thread is not None:
            active = self._current_chart_view()
            if active is not None:
                analytics_thread.update_config(active.config)
        return True

    def _load_workspace(self) -> None:
        loaded = False

        raw = self.settings.value('workspace_state', '')
        if isinstance(raw, str) and raw:
            try:
                loaded = self._apply_workspace_payload(json.loads(raw))
            except Exception:
                loaded = False

        if not loaded:
            raw = self.settings.value('charts', '')
            if isinstance(raw, str) and raw:
                try:
                    legacy_payload = {
                        'charts': json.loads(raw),
                        'active_tab': int(self.settings.value('active_tab', 0)),
                    }
                    loaded = self._apply_workspace_payload(legacy_payload)
                except Exception:
                    loaded = False

        if not loaded:
            self._add_chart_tab(chart_type='market', config=self._default_chart_config(), name='Market 1')

    def _save_workspace_and_notify(self) -> None:
        self._save_workspace()
        QMessageBox.information(self, 'Workspace Saved', 'Workspace settings were saved.')

    def _reload_workspace_and_notify(self) -> None:
        raw = self.settings.value('workspace_state', '')
        if not isinstance(raw, str) or not raw:
            QMessageBox.information(self, 'Workspace', 'No saved workspace was found yet.')
            return
        try:
            loaded = self._apply_workspace_payload(json.loads(raw))
        except Exception:
            loaded = False
        if not loaded:
            QMessageBox.warning(self, 'Workspace', 'Saved workspace could not be loaded.')
            return
        QMessageBox.information(self, 'Workspace Loaded', 'Workspace settings were restored.')

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
