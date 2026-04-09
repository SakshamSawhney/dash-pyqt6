from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pyqtgraph as pg
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import QAbstractItemView, QFrame, QHBoxLayout, QLabel, QVBoxLayout, QWidget
from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QTextEdit


@dataclass
class ComparisonPayload:
    labels: list[str]
    current: list[float]
    compare: list[float]
    mode: str
    range_low: list[float]
    range_high: list[float]
    show_ranges: bool


class CategoryAxisItem(pg.AxisItem):
    def __init__(self, orientation: str = "bottom") -> None:
        super().__init__(orientation=orientation)
        self._labels: list[str] = []

    def set_labels(self, labels: Iterable[str]) -> None:
        self._labels = [str(label) for label in labels]
        if not self._labels:
            self.setTicks([])
            return
        step = 1 if len(self._labels) <= 8 else max(1, int(np.ceil(len(self._labels) / 8)))
        ticks = [(idx, text) for idx, text in enumerate(self._labels) if idx % step == 0 or idx == len(self._labels) - 1]
        if ticks[-1][0] != len(self._labels) - 1:
            ticks.append((len(self._labels) - 1, self._labels[-1]))
        ticks = [ticks]
        self.setTicks(ticks)


class DashboardCard(QWidget):
    def __init__(self, title: str, subtitle: str = "") -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(4)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(4)

        title_layout = QVBoxLayout()
        title_layout.setContentsMargins(0, 0, 0, 0)
        title_layout.setSpacing(1)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("cardTitle")
        title_layout.addWidget(self.title_label)

        self.subtitle_label = QLabel(subtitle)
        self.subtitle_label.setObjectName("cardSubtitle")
        self.subtitle_label.setVisible(bool(subtitle))
        title_layout.addWidget(self.subtitle_label)

        header.addLayout(title_layout, 1)
        self.meta_label = QLabel("")
        self.meta_label.setObjectName("cardMeta")
        self.meta_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        header.addWidget(self.meta_label)
        layout.addLayout(header)

        self.content_layout = QVBoxLayout()
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(0)
        layout.addLayout(self.content_layout, 1)

        self.setObjectName("dashboardCard")
        self._text_size = 12

    def set_meta(self, text: str) -> None:
        self.meta_label.setText(text)

    def set_subtitle(self, text: str) -> None:
        self.subtitle_label.setText(text)
        self.subtitle_label.setVisible(bool(text))

    def set_text_scale(self, text_size: int) -> None:
        self._text_size = int(text_size)


class ComparisonChartWidget(DashboardCard):
    hover_changed = pyqtSignal(dict)

    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__(title, subtitle)
        self.axis = CategoryAxisItem("bottom")
        self.plot = pg.PlotWidget(axisItems={"bottom": self.axis})
        self.plot.setMinimumHeight(180)
        self.plot.setMouseEnabled(x=False, y=True)
        self.plot.showGrid(x=False, y=True, alpha=0.18)
        self.plot.hideButtons()
        self.legend = self.plot.addLegend(offset=(8, 8))
        self.plot.setMenuEnabled(False)
        self.plot.getViewBox().setDefaultPadding(0.08)
        self.plot.getPlotItem().layout.setContentsMargins(4, 4, 4, 4)
        self.content_layout.addWidget(self.plot)

        self._current_curve = self.plot.plot(
            pen=pg.mkPen("#f4f1ea", width=2.0),
            symbol="o",
            symbolSize=7,
            symbolBrush=pg.mkBrush("#f4f1ea"),
            symbolPen=pg.mkPen("#f4f1ea", width=1.0),
            name="Current",
        )
        self._compare_curve = self.plot.plot(
            pen=pg.mkPen(QColor("#7a8189"), width=1.4, style=Qt.PenStyle.DashLine),
            symbol="o",
            symbolSize=5,
            symbolBrush=pg.mkBrush("#7a8189"),
            symbolPen=pg.mkPen("#7a8189", width=1.0),
            name="Selected Date",
        )
        self._range_low_curve = self.plot.plot(
            pen=pg.mkPen(QColor("#ff8f70"), width=1.3, style=Qt.PenStyle.DotLine),
            symbol="t",
            symbolSize=6,
            symbolBrush=pg.mkBrush("#ff8f70"),
            symbolPen=pg.mkPen("#ff8f70", width=1.0),
            name="Range Min",
        )
        self._range_high_curve = self.plot.plot(
            pen=pg.mkPen(QColor("#8de38d"), width=1.3, style=Qt.PenStyle.DotLine),
            symbol="t1",
            symbolSize=6,
            symbolBrush=pg.mkBrush("#8de38d"),
            symbolPen=pg.mkPen("#8de38d", width=1.0),
            name="Range Max",
        )
        self._bars = pg.BarGraphItem(x=[], y0=[], y1=[], width=0.8, brush=pg.mkBrush(255, 255, 255, 40), pen=None)
        self.plot.addItem(self._bars)
        self._baseline = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#70757c", width=1.0))
        self.plot.addItem(self._baseline)
        self._cursor_tip = pg.TextItem("", anchor=(0, 1), fill=pg.mkBrush(12, 14, 18, 230))
        self._cursor_tip.setZValue(20)
        self._cursor_tip.hide()
        self.plot.addItem(self._cursor_tip, ignoreBounds=True)
        self._labels: list[pg.TextItem] = []
        self._payload = ComparisonPayload([], [], [], "actual", [], [], False)
        self._proxy = pg.SignalProxy(self.plot.scene().sigMouseMoved, rateLimit=60, slot=self._on_mouse_moved)
        self._current_compare_label = ""
        self._apply_plot_text_scale()

    def _apply_plot_text_scale(self) -> None:
        axis_font = QFont("Segoe UI", max(7, self._text_size - 3))
        tip_font = QFont("Segoe UI", max(8, self._text_size - 2))
        for axis_name in ("bottom", "left"):
            axis = self.plot.getPlotItem().getAxis(axis_name)
            axis.setStyle(tickFont=axis_font, tickTextOffset=6)
            axis.setTextPen(pg.mkPen("#cfd4da"))
            if axis_name == "bottom":
                axis.setHeight(max(24, self._text_size * 2))
        if self.legend is not None:
            for _sample, label in self.legend.items:
                label.item.setDefaultTextColor(QColor("#d7dbdf"))
                label.item.setFont(axis_font)
        self._cursor_tip.setFont(tip_font)
        self._cursor_tip.setColor("#f3efe6")

    def set_text_scale(self, text_size: int) -> None:
        super().set_text_scale(text_size)
        self._apply_plot_text_scale()
        if self._payload.labels:
            self.update_payload(
                list(self._payload.labels),
                list(self._payload.current),
                list(self._payload.compare),
                self._payload.mode,
                self._current_compare_label,
                list(self._payload.range_low),
                list(self._payload.range_high),
                self._payload.show_ranges,
            )

    def _clear_labels(self) -> None:
        for item in self._labels:
            self.plot.removeItem(item)
        self._labels.clear()

    def _place_cursor_tip(self, mouse_point: pg.Point, anchor_y: float = 0.0) -> None:
        vb = self.plot.getPlotItem().getViewBox()
        x_range, y_range = vb.viewRange()
        x_span = max(1.0, float(x_range[1] - x_range[0]))
        y_span = max(1.0, float(y_range[1] - y_range[0]))

        x_margin = 0.03 * x_span
        y_margin = 0.05 * y_span
        x_pos = float(mouse_point.x()) + x_margin
        y_pos = float(mouse_point.y()) + anchor_y
        anchor = (0.0, 1.0)

        if x_pos > (x_range[1] - x_margin):
            x_pos = float(mouse_point.x()) - x_margin
            anchor = (1.0, anchor[1])

        if y_pos > (y_range[1] - y_margin):
            y_pos = float(mouse_point.y()) - y_margin
            anchor = (anchor[0], 0.0)
        elif y_pos < (y_range[0] + y_margin):
            y_pos = float(mouse_point.y()) + y_margin
            anchor = (anchor[0], 1.0)

        self._cursor_tip.setAnchor(anchor)
        self._cursor_tip.setPos(x_pos, y_pos)

    def update_payload(
        self,
        labels: list[str],
        current_values: list[float],
        compare_values: list[float],
        mode: str,
        compare_label: str,
        range_low_values: list[float] | None = None,
        range_high_values: list[float] | None = None,
        show_ranges: bool = False,
    ) -> None:
        range_low = list(range_low_values or [])
        range_high = list(range_high_values or [])
        self._payload = ComparisonPayload(labels, current_values, compare_values, mode, range_low, range_high, show_ranges)
        self._current_compare_label = compare_label
        self.axis.set_labels(labels)
        self._clear_labels()

        if not labels or not current_values:
            self._current_curve.setData([], [])
            self._compare_curve.setData([], [])
            self._range_low_curve.setData([], [])
            self._range_high_curve.setData([], [])
            self._bars.setOpts(x=[], y0=[], y1=[], width=0.8)
            self._cursor_tip.hide()
            self.set_meta("No data")
            return

        x = np.arange(len(labels), dtype=float)
        current_arr = np.array(current_values, dtype=float)
        compare_arr = np.array(compare_values, dtype=float)
        mask = np.isfinite(current_arr)
        compare_mask = np.isfinite(compare_arr)

        if mode == "change":
            current_plot = current_arr - compare_arr
            compare_plot = np.zeros_like(compare_arr)
            label_values = current_plot
            y0 = np.zeros_like(current_plot)
            y1 = current_plot
            compact_label = compare_label[5:] if len(compare_label) >= 10 and compare_label[4] == "-" else compare_label
            self.set_subtitle(f"dChg vs {compact_label}")
            self._baseline.setPos(0.0)
        else:
            current_plot = current_arr
            compare_plot = compare_arr
            label_values = current_arr
            y0 = compare_arr
            y1 = current_arr
            compact_label = compare_label[5:] if len(compare_label) >= 10 and compare_label[4] == "-" else compare_label
            self.set_subtitle(f"Lvl vs {compact_label}")
            finite = compare_arr[np.isfinite(compare_arr)]
            self._baseline.setPos(float(np.nanmedian(finite)) if finite.size else 0.0)

        bar_brush = pg.mkBrush(66, 160, 255, 70) if mode == "change" else pg.mkBrush(255, 255, 255, 38)
        self._bars.setOpts(x=x, y0=y0, y1=y1, width=0.78, brush=bar_brush)
        self._current_curve.setData(x[mask], current_plot[mask])
        self._compare_curve.setData(x[compare_mask], compare_plot[compare_mask])
        show_range_overlay = show_ranges and mode != "change" and len(range_low) == len(labels) and len(range_high) == len(labels)
        if show_range_overlay:
            low_arr = np.array(range_low, dtype=float)
            high_arr = np.array(range_high, dtype=float)
            low_mask = np.isfinite(low_arr)
            high_mask = np.isfinite(high_arr)
            self._range_low_curve.setData(x[low_mask], low_arr[low_mask])
            self._range_high_curve.setData(x[high_mask], high_arr[high_mask])
        else:
            self._range_low_curve.setData([], [])
            self._range_high_curve.setData([], [])

        font = QFont("Segoe UI", max(7, self._text_size - 2))
        for idx, value in enumerate(label_values):
            if not np.isfinite(value):
                continue
            text = f"{value:.1f}" if abs(value) >= 1.0 else f"{value:.4f}".rstrip("0").rstrip(".")
            label = pg.TextItem(text, color="#36f1ff", anchor=(0.5, 1.0))
            label.setFont(font)
            y_offset = 0.02 * max(1.0, float(np.nanmax(np.abs(label_values[np.isfinite(label_values)]))))
            label.setPos(float(idx), float(value) + y_offset)
            self.plot.addItem(label)
            self._labels.append(label)

        latest_text = f"{labels[-1]}  {current_arr[-1]:.4f}" if np.isfinite(current_arr[-1]) else "Waiting for values"
        self.set_meta(latest_text)
        self.plot.enableAutoRange(axis="xy", enable=True)

    def _on_mouse_moved(self, evt) -> None:
        if not self._payload.labels:
            return
        pos = evt[0]
        if not self.plot.sceneBoundingRect().contains(pos):
            self._cursor_tip.hide()
            return
        mouse_point = self.plot.getPlotItem().getViewBox().mapSceneToView(pos)
        idx = int(round(mouse_point.x()))
        idx = max(0, min(idx, len(self._payload.labels) - 1))
        value = self._payload.current[idx] if idx < len(self._payload.current) else None
        compare = self._payload.compare[idx] if idx < len(self._payload.compare) else None
        display_value = None
        compare_value = None
        if value is not None and compare is not None:
            display_value = value - compare if self._payload.mode == "change" else value
            compare_value = 0.0 if self._payload.mode == "change" else compare
        tooltip_lines = [self._payload.labels[idx]]
        if display_value is not None:
            tooltip_lines.append(f"Now {display_value:.4f}")
        if compare_value is not None:
            tooltip_lines.append(f"Ref {compare_value:.4f}")
        if self._payload.show_ranges and self._payload.mode != "change":
            low = self._payload.range_low[idx] if idx < len(self._payload.range_low) else None
            high = self._payload.range_high[idx] if idx < len(self._payload.range_high) else None
            if low is not None and np.isfinite(low):
                tooltip_lines.append(f"Min {float(low):.4f}")
            if high is not None and np.isfinite(high):
                tooltip_lines.append(f"Max {float(high):.4f}")
        self._cursor_tip.setText("\n".join(tooltip_lines))
        self._place_cursor_tip(mouse_point)
        self._cursor_tip.show()
        self.hover_changed.emit(
            {
                "series_name": self.title_label.text(),
                "x_label": self._payload.labels[idx],
                "value": value,
            }
        )


class ZScoreSnapshotWidget(DashboardCard):
    hover_changed = pyqtSignal(dict)

    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__(title, subtitle)
        self.axis = CategoryAxisItem("bottom")
        self.plot = pg.PlotWidget(axisItems={"bottom": self.axis})
        self.plot.setMinimumHeight(180)
        self.plot.setMouseEnabled(x=False, y=True)
        self.plot.showGrid(x=False, y=True, alpha=0.18)
        self.plot.hideButtons()
        self.plot.setMenuEnabled(False)
        self.plot.getPlotItem().layout.setContentsMargins(4, 4, 4, 4)
        self.content_layout.addWidget(self.plot)

        self._bars = pg.BarGraphItem(x=[], y0=[], y1=[], width=0.76, brush=pg.mkBrush(67, 212, 146, 110), pen=None)
        self.plot.addItem(self._bars)
        self._line = self.plot.plot(
            pen=pg.mkPen("#9bf2d1", width=2.0),
            symbol="o",
            symbolSize=7,
            symbolBrush=pg.mkBrush("#9bf2d1"),
            symbolPen=pg.mkPen("#9bf2d1", width=1.0),
        )
        self._zero = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#7a8189", width=1.0))
        self._pos_sigma = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#ff8a65", width=1.0, style=Qt.PenStyle.DashLine))
        self._neg_sigma = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen("#ff8a65", width=1.0, style=Qt.PenStyle.DashLine))
        self.plot.addItem(self._zero)
        self.plot.addItem(self._pos_sigma)
        self.plot.addItem(self._neg_sigma)
        self._cursor_tip = pg.TextItem("", anchor=(0, 1), fill=pg.mkBrush(12, 14, 18, 230))
        self._cursor_tip.setZValue(20)
        self._cursor_tip.hide()
        self.plot.addItem(self._cursor_tip, ignoreBounds=True)
        self._labels: list[pg.TextItem] = []
        self._payload: list[tuple[str, float]] = []
        self._proxy = pg.SignalProxy(self.plot.scene().sigMouseMoved, rateLimit=60, slot=self._on_mouse_moved)
        self._sigma_level = 2.0
        self._apply_plot_text_scale()

    def _apply_plot_text_scale(self) -> None:
        axis_font = QFont("Segoe UI", max(7, self._text_size - 3))
        tip_font = QFont("Segoe UI", max(8, self._text_size - 2))
        for axis_name in ("bottom", "left"):
            axis = self.plot.getPlotItem().getAxis(axis_name)
            axis.setStyle(tickFont=axis_font, tickTextOffset=6)
            axis.setTextPen(pg.mkPen("#cfd4da"))
            if axis_name == "bottom":
                axis.setHeight(max(24, self._text_size * 2))
        self._cursor_tip.setFont(tip_font)
        self._cursor_tip.setColor("#f3efe6")

    def set_text_scale(self, text_size: int) -> None:
        super().set_text_scale(text_size)
        self._apply_plot_text_scale()
        if self._payload:
            labels = [label for label, _ in self._payload]
            values = [value for _, value in self._payload]
            self.update_payload(labels, values, self._sigma_level)

    def _clear_labels(self) -> None:
        for item in self._labels:
            self.plot.removeItem(item)
        self._labels.clear()

    def _place_cursor_tip(self, mouse_point: pg.Point) -> None:
        vb = self.plot.getPlotItem().getViewBox()
        x_range, y_range = vb.viewRange()
        x_span = max(1.0, float(x_range[1] - x_range[0]))
        y_span = max(1.0, float(y_range[1] - y_range[0]))

        x_margin = 0.03 * x_span
        y_margin = 0.05 * y_span
        x_pos = float(mouse_point.x()) + x_margin
        y_pos = float(mouse_point.y())
        anchor = (0.0, 1.0)

        if x_pos > (x_range[1] - x_margin):
            x_pos = float(mouse_point.x()) - x_margin
            anchor = (1.0, anchor[1])

        if y_pos > (y_range[1] - y_margin):
            y_pos = float(mouse_point.y()) - y_margin
            anchor = (anchor[0], 0.0)
        elif y_pos < (y_range[0] + y_margin):
            y_pos = float(mouse_point.y()) + y_margin
            anchor = (anchor[0], 1.0)

        self._cursor_tip.setAnchor(anchor)
        self._cursor_tip.setPos(x_pos, y_pos)

    def update_payload(self, labels: list[str], zscores: list[float], sigma_level: float) -> None:
        self._sigma_level = float(sigma_level)
        self.axis.set_labels(labels)
        self._payload = list(zip(labels, zscores, strict=False))
        self._clear_labels()
        if not labels:
            self._bars.setOpts(x=[], y0=[], y1=[], width=0.76)
            self._line.setData([], [])
            self._cursor_tip.hide()
            self.set_meta("No z-score data")
            return

        x = np.arange(len(labels), dtype=float)
        values = np.array(zscores, dtype=float)
        brush = [pg.mkBrush("#ff6b6b" if abs(val) >= sigma_level else "#43d492") for val in values]
        self._bars.setOpts(x=x, y0=np.zeros_like(values), y1=values, width=0.76, brushes=brush)
        self._line.setData(x, values)
        self._pos_sigma.setPos(float(sigma_level))
        self._neg_sigma.setPos(float(-sigma_level))

        font = QFont("Segoe UI", max(7, self._text_size - 2))
        max_abs = max(1.0, float(np.nanmax(np.abs(values))))
        for idx, value in enumerate(values):
            text = f"{value:.2f}"
            anchor = (0.5, 1.0) if value >= 0 else (0.5, 0.0)
            label = pg.TextItem(text, color="#eaf7f3", anchor=anchor)
            label.setFont(font)
            offset = 0.05 * max_abs
            label.setPos(float(idx), float(value) + (-offset if value >= 0 else offset))
            self.plot.addItem(label)
            self._labels.append(label)

        hottest = max(self._payload, key=lambda item: abs(item[1]))
        self.set_meta(f"Max |z| {hottest[0]}  {hottest[1]:.2f}")
        self.plot.enableAutoRange(axis="xy", enable=True)

    def _on_mouse_moved(self, evt) -> None:
        if not self._payload:
            return
        pos = evt[0]
        if not self.plot.sceneBoundingRect().contains(pos):
            self._cursor_tip.hide()
            return
        mouse_point = self.plot.getPlotItem().getViewBox().mapSceneToView(pos)
        idx = int(round(mouse_point.x()))
        idx = max(0, min(idx, len(self._payload) - 1))
        label, value = self._payload[idx]
        self._cursor_tip.setText(f"{label}\nZ {value:.4f}")
        self._place_cursor_tip(mouse_point)
        self._cursor_tip.show()
        self.hover_changed.emit({"series_name": self.title_label.text(), "x_label": label, "value": value})


class SignalLabWidget(DashboardCard):
    def __init__(self, title: str, subtitle: str) -> None:
        super().__init__(title, subtitle)

        self.frame = QFrame()
        self.frame.setFrameShape(QFrame.Shape.StyledPanel)
        inner = QVBoxLayout(self.frame)
        inner.setContentsMargins(12, 12, 12, 12)
        inner.setSpacing(8)

        self.signal_state = QLabel("Signal stack not active yet")
        self.signal_state.setObjectName("signalHeadline")
        inner.addWidget(self.signal_state)

        self.summary = QLabel(
            "This panel is reserved for TAS, seasonality, lead/lag, volume bursts and signal scoring."
        )
        self.summary.setWordWrap(True)
        self.summary.setObjectName("signalCopy")
        inner.addWidget(self.summary)

        self.metric_labels: dict[str, QLabel] = {}
        for label in ["Top Idea", "Regime", "Lead/Lag", "Execution"]:
            row = QHBoxLayout()
            row.setContentsMargins(0, 0, 0, 0)
            name = QLabel(label)
            name.setObjectName("signalMetricName")
            value = QLabel("Pending")
            value.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            value.setObjectName("signalMetricValue")
            row.addWidget(name)
            row.addStretch(1)
            row.addWidget(value)
            inner.addLayout(row)
            self.metric_labels[label] = value

        self.candidate_table = QTableWidget(0, 13)
        self.candidate_table.setHorizontalHeaderLabels(
            ["Trade", "Side", "Type", "Z", "Pctile", "Half-Life", "MR", "Regime", "Lead/Lag", "Flow", "Hold", "Entry", "Conf"]
        )
        self.candidate_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.candidate_table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.candidate_table.verticalHeader().setVisible(False)
        self.candidate_table.setMinimumHeight(180)
        self.candidate_table.setAlternatingRowColors(True)
        self.candidate_table.setShowGrid(False)
        self.candidate_table.setSortingEnabled(True)
        self.candidate_table.horizontalHeader().setStretchLastSection(False)
        self.candidate_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        inner.addWidget(self.candidate_table)

        self.notes_detail = QTextEdit()
        self.notes_detail.setReadOnly(True)
        self.notes_detail.setMinimumHeight(180)
        self.notes_detail.setObjectName("signalCopy")
        self.notes_detail.setPlainText("Signal engine waiting for historical and live data.")
        inner.addWidget(self.notes_detail)

        inner.addStretch(1)
        self.content_layout.addWidget(self.frame)
        self._apply_workbench_style()

    def _apply_workbench_style(self) -> None:
        header = self.candidate_table.horizontalHeader()
        header.setDefaultSectionSize(96)
        header.resizeSection(0, 170)
        header.resizeSection(1, 60)
        header.resizeSection(2, 78)
        header.resizeSection(3, 56)
        header.resizeSection(4, 62)
        header.resizeSection(5, 72)
        header.resizeSection(6, 64)
        header.resizeSection(7, 76)
        header.resizeSection(8, 110)
        header.resizeSection(9, 82)
        header.resizeSection(10, 70)
        header.resizeSection(11, 92)
        header.resizeSection(12, 60)
        font = QFont("Segoe UI", 9)
        self.candidate_table.setFont(font)
        self.notes_detail.setFont(QFont("Segoe UI", 10))
        self.notes_detail.setStyleSheet("line-height: 1.35;")

    @staticmethod
    def _set_item_style(item: QTableWidgetItem, text: str, column_name: str, row_rank: int) -> None:
        value = str(text)
        item.setText(value)
        item.setForeground(QColor("#ece7dc"))
        if row_rank == 0:
            item.setBackground(QColor("#16222b"))
        elif row_rank == 1:
            item.setBackground(QColor("#151d22"))
        elif row_rank == 2:
            item.setBackground(QColor("#14191d"))

        if column_name == "Trade":
            if "Receive" in value or "Buy belly" in value:
                item.setForeground(QColor("#4aa3ff"))
            elif "Pay" in value or "Sell belly" in value:
                item.setForeground(QColor("#ff6b6b"))
            item.setFont(QFont("Segoe UI", 9, QFont.Weight.DemiBold))
        elif column_name == "Side":
            if value == "Buy":
                item.setForeground(QColor("#4aa3ff"))
                item.setFont(QFont("Segoe UI", 9, QFont.Weight.DemiBold))
            elif value == "Sell":
                item.setForeground(QColor("#ff6b6b"))
                item.setFont(QFont("Segoe UI", 9, QFont.Weight.DemiBold))
        elif column_name == "Z":
            if value.startswith("+"):
                item.setForeground(QColor("#ff6b6b"))
            elif value.startswith("-"):
                item.setForeground(QColor("#4aa3ff"))
        elif column_name == "MR":
            color_map = {
                "Strong": "#67d7a7",
                "Good": "#8fdca9",
                "Moderate": "#f0c36b",
                "Slow": "#ff9f6e",
                "Weak": "#ff6b6b",
            }
            item.setForeground(QColor(color_map.get(value, "#ece7dc")))
        elif column_name == "Regime":
            color_map = {
                "Low Vol": "#67d7a7",
                "Balanced": "#f0c36b",
                "High Vol": "#ff6b6b",
            }
            item.setForeground(QColor(color_map.get(value, "#ece7dc")))
        elif column_name == "Lead/Lag":
            if "leads" in value:
                item.setForeground(QColor("#67d7a7"))
            elif "contra" in value.lower():
                item.setForeground(QColor("#ff6b6b"))
            elif value == "Weak":
                item.setForeground(QColor("#9ea6ad"))
        elif column_name == "Flow":
            color_map = {
                "Supportive": "#4aa3ff",
                "Mixed": "#f0c36b",
                "Against": "#ff6b6b",
                "Neutral": "#9ea6ad",
            }
            item.setForeground(QColor(color_map.get(value, "#ece7dc")))
        elif column_name == "Conf":
            try:
                numeric = float(value.replace("%", ""))
            except Exception:
                numeric = None
            if numeric is not None:
                if numeric >= 75:
                    item.setForeground(QColor("#67d7a7"))
                elif numeric >= 55:
                    item.setForeground(QColor("#f0c36b"))
                else:
                    item.setForeground(QColor("#ff6b6b"))

    def update_payload(self, payload: dict) -> None:
        self.signal_state.setText(payload.get("headline", "Signals idle"))
        self.summary.setText(payload.get("summary", "Waiting for historical and live data."))
        self.metric_labels["Top Idea"].setText(payload.get("top_metric", "n/a"))
        self.metric_labels["Regime"].setText(payload.get("regime_metric", "n/a"))
        self.metric_labels["Lead/Lag"].setText(payload.get("leadlag_metric", "n/a"))
        self.metric_labels["Execution"].setText(payload.get("execution_metric", "n/a"))
        rows = payload.get("rows", [])
        self.candidate_table.setSortingEnabled(False)
        self.candidate_table.clearContents()
        self.candidate_table.setRowCount(len(rows))
        column_names = ["Trade", "Side", "Type", "Z", "Pctile", "Half-Life", "MR", "Regime", "Lead/Lag", "Flow", "Hold", "Entry", "Conf"]
        for row_idx, row in enumerate(rows):
            values = [
                str(row.get("trade", "")),
                str(row.get("side", "")),
                str(row.get("type", "")),
                str(row.get("zscore", "")),
                str(row.get("percentile", "")),
                str(row.get("half_life", "")),
                str(row.get("mr_strength", "")),
                str(row.get("regime", "")),
                str(row.get("leadlag", "")),
                str(row.get("flow", "")),
                str(row.get("holding", "")),
                str(row.get("entry", "")),
                str(row.get("confidence", "")),
            ]
            for col_idx, (column_name, value) in enumerate(zip(column_names, values, strict=False)):
                item = QTableWidgetItem()
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter if col_idx > 2 else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                self._set_item_style(item, value, column_name, row_idx)
                self.candidate_table.setItem(row_idx, col_idx, item)
        self.candidate_table.setSortingEnabled(True)
        self.candidate_table.sortItems(12, Qt.SortOrder.DescendingOrder)
        self.notes_detail.setHtml(payload.get("notes_detail", "Signal engine waiting for historical and live data."))
        self.set_meta(payload.get("meta", "Signals"))
