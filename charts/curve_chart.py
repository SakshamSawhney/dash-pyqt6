from __future__ import annotations

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt6.QtCore import QPointF
from PyQt6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QVBoxLayout, QWidget


class CurveChartWidget(QWidget):
    _PALETTE = [
        '#1f77b4',
        '#d62728',
        '#2ca02c',
        '#ff7f0e',
        '#9467bd',
        '#8c564b',
        '#17becf',
        '#e377c2',
    ]

    def __init__(self, title: str = 'Yield Curve') -> None:
        super().__init__()
        layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        self.x_zoom_chk = QCheckBox('X Zoom')
        self.y_zoom_chk = QCheckBox('Y Zoom')
        self.x_zoom_chk.setChecked(True)
        self.y_zoom_chk.setChecked(True)
        controls.addWidget(self.x_zoom_chk)
        controls.addWidget(self.y_zoom_chk)
        self.hover_info_label = QLabel('X: n/a | Rate: n/a')
        controls.addWidget(self.hover_info_label)
        controls.addStretch(1)
        layout.addLayout(controls)

        self.plot = pg.PlotWidget(title=title)
        self.plot.addLegend()
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        layout.addWidget(self.plot)

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

        self._curves: dict[str, pg.PlotDataItem] = {}
        self._series_cache: dict[str, np.ndarray] = {}
        self._x_cache = np.array([], dtype=float)
        self._labels: list[str] = []

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
        except (TypeError, ValueError):
            return

    def _apply_zoom_mode(self) -> None:
        vb = self.plot.getPlotItem().getViewBox()
        vb.setMouseEnabled(x=self.x_zoom_chk.isChecked(), y=self.y_zoom_chk.isChecked())

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
        label = self._labels[idx] if idx < len(self._labels) else ''
        nearest_curve = None
        nearest_y = np.nan
        nearest_dist = np.inf
        lines = []
        for name, y_arr in self._series_cache.items():
            if idx >= len(y_arr):
                continue
            yv = y_arr[idx]
            if not np.isfinite(yv):
                continue
            lines.append(f'{name}: {float(yv):.4f}')
            dist = abs(float(yv) - float(mouse_point.y()))
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_curve = name
                nearest_y = float(yv)
        if not np.isfinite(nearest_y):
            return
        y_val = nearest_y

        self._vline.setPos(x_val)
        self._hline.setPos(y_val)

        xr, yr = vb.viewRange()
        head = f'X: {label}'
        if nearest_curve:
            head += f'\n{nearest_curve}: {y_val:.4f}'
        self._hover_text.setText('\n'.join([head] + lines[:8]))
        self._hover_text.setPos(xr[0], yr[1])

        self._x_axis_label.setText(f'X: {label}')
        self._x_axis_label.setPos(x_val, yr[0])

        self._y_axis_label.setText(f'{y_val:.4f}')
        self._y_axis_label.setPos(xr[0], y_val)
        self.hover_info_label.setText(f'X: {label} | Rate: {y_val:.4f}')

    def update_curves_map(self, curves: dict[str, pd.DataFrame]) -> None:
        non_empty = {name: df for name, df in curves.items() if isinstance(df, pd.DataFrame) and not df.empty}
        if not non_empty:
            for item in list(self._curves.values()):
                self.plot.removeItem(item)
            self._curves.clear()
            self._series_cache.clear()
            self._x_cache = np.array([], dtype=float)
            self._labels = []
            return

        first_df = next(iter(non_empty.values())).reset_index(drop=True)
        labels = [str(v) for v in first_df['instrument'].tolist()]
        x = np.arange(len(labels), dtype=float)
        self._x_cache = x
        self._labels = labels

        active_names = set(non_empty.keys())
        for name in list(self._curves.keys()):
            if name not in active_names:
                self.plot.removeItem(self._curves.pop(name))
                self._series_cache.pop(name, None)

        for idx, (name, curve_df) in enumerate(non_empty.items()):
            color = self._PALETTE[idx % len(self._PALETTE)]
            if name not in self._curves:
                self._curves[name] = self.plot.plot(
                    name=name,
                    pen=pg.mkPen(color=color, width=2),
                    symbol='o',
                    symbolSize=6,
                )
            else:
                self._curves[name].setPen(pg.mkPen(color=color, width=2))

            aligned = pd.Series(index=labels, dtype=float)
            values = curve_df.set_index('instrument')['rate'].astype(float)
            aligned.loc[values.index.astype(str)] = values.values
            y = aligned.to_numpy(dtype=float)
            self._series_cache[name] = y
            mask = np.isfinite(y)
            self._curves[name].setData(x=x[mask], y=y[mask])

        ticks = [(i, lbl) for i, lbl in enumerate(labels)]
        self.plot.getAxis('bottom').setTicks([ticks])

    def update_curve(self, curve_df: pd.DataFrame) -> None:
        self.update_curves_map({'Live': curve_df})
