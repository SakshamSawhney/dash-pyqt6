from __future__ import annotations

import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt6.QtCore import QPointF
from PyQt6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QVBoxLayout, QWidget


class CurveChartWidget(QWidget):
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
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        self.scatter = self.plot.plot(pen=pg.mkPen(width=2), symbol='o', symbolSize=7)
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

        self._x_cache = np.array([], dtype=float)
        self._y_cache = np.array([], dtype=float)
        self._labels: list[str] = []

        self.x_zoom_chk.toggled.connect(self._apply_zoom_mode)
        self.y_zoom_chk.toggled.connect(self._apply_zoom_mode)
        self._apply_zoom_mode()

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
        y_val = float(self._y_cache[idx])
        label = self._labels[idx] if idx < len(self._labels) else ''

        self._vline.setPos(x_val)
        self._hline.setPos(y_val)

        xr, yr = vb.viewRange()
        self._hover_text.setText(f'X: {label}\nRate: {y_val:.4f}')
        self._hover_text.setPos(xr[0], yr[1])

        self._x_axis_label.setText(f'X: {label}')
        self._x_axis_label.setPos(x_val, yr[0])

        self._y_axis_label.setText(f'{y_val:.4f}')
        self._y_axis_label.setPos(xr[0], y_val)
        self.hover_info_label.setText(f'X: {label} | Rate: {y_val:.4f}')

    def update_curve(self, curve_df: pd.DataFrame) -> None:
        if curve_df.empty:
            self.scatter.setData([], [])
            self._x_cache = np.array([], dtype=float)
            self._y_cache = np.array([], dtype=float)
            self._labels = []
            return

        x = np.arange(len(curve_df), dtype=float)
        y = curve_df['rate'].astype(float).to_numpy()
        self.scatter.setData(x=x, y=y)

        self._x_cache = x
        self._y_cache = y
        self._labels = [str(v) for v in curve_df['instrument'].tolist()]

        ticks = [(idx, row['instrument']) for idx, row in curve_df.reset_index(drop=True).iterrows()]
        axis = self.plot.getAxis('bottom')
        axis.setTicks([ticks])