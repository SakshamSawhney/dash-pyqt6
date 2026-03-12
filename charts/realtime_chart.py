from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt6.QtCore import QPointF
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QVBoxLayout, QWidget

from charts.date_axis import DateIndexAxisItem


class RealtimeChartWidget(QWidget):
    _PALETTE = [
        '#1f77b4',
        '#ff7f0e',
        '#2ca02c',
        '#d62728',
        '#9467bd',
        '#8c564b',
        '#e377c2',
        '#7f7f7f',
        '#bcbd22',
        '#17becf',
        '#e41a1c',
        '#377eb8',
        '#4daf4a',
        '#984ea3',
        '#ff7f00',
    ]

    def __init__(self, title: str = 'Realtime Rates') -> None:
        super().__init__()
        layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        self.x_zoom_chk = QCheckBox('X Zoom')
        self.y_zoom_chk = QCheckBox('Y Zoom')
        self.x_zoom_chk.setChecked(True)
        self.y_zoom_chk.setChecked(True)
        controls.addWidget(self.x_zoom_chk)
        controls.addWidget(self.y_zoom_chk)
        self.hover_info_label = QLabel('X: n/a | Y: n/a')
        controls.addWidget(self.hover_info_label)
        controls.addStretch(1)
        layout.addLayout(controls)

        self.date_axis = DateIndexAxisItem(orientation='bottom')
        self.plot = pg.PlotWidget(title=title, axisItems={'bottom': self.date_axis})
        self.plot.addLegend()
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        self.plot.setDownsampling(auto=True, mode='peak')
        self.plot.setClipToView(True)
        layout.addWidget(self.plot)

        self._curves: dict[str, pg.PlotDataItem] = {}
        self._series_cache: dict[str, np.ndarray] = {}
        self._color_map: dict[str, str] = {}
        self._x_cache = np.array([], dtype=float)
        self._index_labels: list[str] = []
        self._has_drawn = False

        self._vline = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('#888888', width=1))
        self._hline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('#666666', width=1))
        self._hover_text = pg.TextItem('', anchor=(0, 1), fill=pg.mkBrush(0, 0, 0, 160))
        self._x_axis_label = pg.TextItem('', anchor=(0.5, 0), fill=pg.mkBrush(20, 20, 20, 220))
        self._y_axis_label = pg.TextItem('', anchor=(0, 0.5), fill=pg.mkBrush(20, 20, 20, 220))

        self.plot.addItem(self._vline, ignoreBounds=True)
        self.plot.addItem(self._hline, ignoreBounds=True)
        self.plot.addItem(self._hover_text, ignoreBounds=True)
        self.plot.addItem(self._x_axis_label, ignoreBounds=True)
        self.plot.addItem(self._y_axis_label, ignoreBounds=True)
        self._proxy = pg.SignalProxy(self.plot.scene().sigMouseMoved, rateLimit=60, slot=self._on_mouse_moved)

        self.x_zoom_chk.toggled.connect(self._apply_zoom_mode)
        self.y_zoom_chk.toggled.connect(self._apply_zoom_mode)
        self._apply_zoom_mode()

    def export_view_state(self) -> dict:
        vb = self.plot.getPlotItem().getViewBox()
        x_range, y_range = vb.viewRange()
        return {
            'x_zoom_enabled': self.x_zoom_chk.isChecked(),
            'y_zoom_enabled': self.y_zoom_chk.isChecked(),
            'view_range': {
                'x': [float(x_range[0]), float(x_range[1])],
                'y': [float(y_range[0]), float(y_range[1])],
            },
        }

    def restore_view_state(self, state: dict | None) -> None:
        if not isinstance(state, dict):
            return
        self.x_zoom_chk.setChecked(bool(state.get('x_zoom_enabled', True)))
        self.y_zoom_chk.setChecked(bool(state.get('y_zoom_enabled', True)))
        view_range = state.get('view_range', {})
        if not isinstance(view_range, dict):
            return
        x_range = view_range.get('x')
        y_range = view_range.get('y')
        if not (
            isinstance(x_range, list)
            and len(x_range) == 2
            and isinstance(y_range, list)
            and len(y_range) == 2
        ):
            return
        try:
            self.plot.setXRange(float(x_range[0]), float(x_range[1]), padding=0.0)
            self.plot.setYRange(float(y_range[0]), float(y_range[1]), padding=0.0)
            self._has_drawn = True
        except (TypeError, ValueError):
            return

    def _apply_zoom_mode(self) -> None:
        vb = self.plot.getPlotItem().getViewBox()
        vb.setMouseEnabled(x=self.x_zoom_chk.isChecked(), y=self.y_zoom_chk.isChecked())

    def _pen_for_name(self, name: str):
        if name not in self._color_map:
            used = set(self._color_map.values())
            color = None
            for c in self._PALETTE:
                if c not in used:
                    color = c
                    break
            if color is None:
                color = self._PALETTE[len(self._color_map) % len(self._PALETTE)]
            self._color_map[name] = color
        return pg.mkPen(QColor(self._color_map[name]), width=2)

    def _on_mouse_moved(self, evt) -> None:
        if len(self._x_cache) == 0:
            return

        pos = evt[0]
        vb = self.plot.getPlotItem().getViewBox()
        if not self.plot.sceneBoundingRect().contains(pos):
            return

        mouse_point: QPointF = vb.mapSceneToView(pos)
        idx = int(round(mouse_point.x()))
        idx = max(0, min(idx, len(self._x_cache) - 1))
        x_val = self._x_cache[idx]

        nearest_name = None
        nearest_y = np.nan
        nearest_dist = np.inf
        values_lines = []

        for name in sorted(self._series_cache.keys()):
            curve = self._curves.get(name)
            if curve is None or not curve.isVisible():
                continue
            y_arr = self._series_cache[name]
            if idx >= len(y_arr):
                continue
            yv = y_arr[idx]
            if not np.isfinite(yv):
                continue
            values_lines.append(f'{name}: {yv:.4f}')
            dist = abs(float(yv) - float(mouse_point.y()))
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_name = name
                nearest_y = float(yv)

        y_display = nearest_y if np.isfinite(nearest_y) else float(mouse_point.y())
        date_lbl = self.date_axis.label_for_index(idx)
        if not date_lbl:
            date_lbl = self._index_labels[idx] if idx < len(self._index_labels) else str(idx)

        self._vline.setPos(x_val)
        self._hline.setPos(y_display)

        xr, yr = vb.viewRange()
        title = f'X: {date_lbl}'
        if nearest_name is not None and np.isfinite(nearest_y):
            title += f'\n{nearest_name}: {nearest_y:.4f}'
        text_lines = [title] + values_lines[:10]
        self._hover_text.setText('\n'.join(text_lines))
        self._hover_text.setPos(xr[0], yr[1])

        self._x_axis_label.setText(f'X: {date_lbl}')
        self._x_axis_label.setPos(x_val, yr[0])

        self._y_axis_label.setText(f'{y_display:.4f}')
        self._y_axis_label.setPos(xr[0], y_display)
        self.hover_info_label.setText(f'X: {date_lbl} | Y: {y_display:.4f}')

    def update_from_pivot(self, pivot_df: pd.DataFrame, series_names: Iterable[str], max_points: int = 3000) -> None:
        if pivot_df.empty:
            return

        tail = pivot_df.tail(max_points)
        x = np.arange(len(tail), dtype=float)
        self._x_cache = x
        self._index_labels = [str(v) for v in tail.index.tolist()]
        self.date_axis.set_index_labels(list(tail.index))

        updated_any = False
        self._series_cache = {}
        for name in series_names:
            if name not in tail.columns:
                continue

            y_full = pd.to_numeric(tail[name], errors='coerce').to_numpy(dtype=float)
            self._series_cache[name] = y_full

            mask = np.isfinite(y_full)
            if not mask.any():
                continue

            updated_any = True
            if name not in self._curves:
                self._curves[name] = self.plot.plot(name=name, pen=self._pen_for_name(name))
            else:
                self._curves[name].setPen(self._pen_for_name(name))
            self._curves[name].setData(x=x[mask], y=y_full[mask])

        if updated_any and not self._has_drawn:
            self.plot.enableAutoRange(axis='xy', enable=True)
            self._has_drawn = True

    def clear_missing(self, active_names: set[str]) -> None:
        for name in list(self._curves.keys()):
            if name not in active_names:
                item = self._curves.pop(name)
                self.plot.removeItem(item)
                self._series_cache.pop(name, None)
