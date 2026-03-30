from __future__ import annotations

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class DashboardControlPanel(QWidget):
    config_changed = pyqtSignal()
    load_api_history_requested = pyqtSignal()
    theme_changed = pyqtSignal(str)
    reset_layout_requested = pyqtSignal()
    focus_mode_requested = pyqtSignal()

    def __init__(self) -> None:
        super().__init__()
        self._history_dates: list[str] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        hero = QVBoxLayout()
        title = QLabel("Rates Workspace")
        title.setObjectName("panelHeroTitle")
        subtitle = QLabel("Six linked panels for outrights, spreads and signal-building.")
        subtitle.setWordWrap(True)
        subtitle.setObjectName("panelHeroCopy")
        hero.addWidget(title)
        hero.addWidget(subtitle)
        root.addLayout(hero)

        self.instrument_list = QListWidget()
        self.instrument_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.instrument_list.itemSelectionChanged.connect(self.config_changed.emit)

        instruments_box = QGroupBox("Contracts")
        instruments_layout = QVBoxLayout(instruments_box)
        instruments_layout.addWidget(self.instrument_list)
        root.addWidget(instruments_box)

        self.compare_date_combo = QComboBox()
        self.compare_date_combo.currentTextChanged.connect(self.config_changed.emit)
        self.y_axis_mode_combo = QComboBox()
        self.y_axis_mode_combo.addItems(["Actual", "Change"])
        self.y_axis_mode_combo.currentTextChanged.connect(self.config_changed.emit)
        self.live_price_mode_combo = QComboBox()
        self.live_price_mode_combo.addItems(["Last", "VWAP"])
        self.live_price_mode_combo.currentTextChanged.connect(self.config_changed.emit)
        self.z_window_spin = QSpinBox()
        self.z_window_spin.setRange(14, 2000)
        self.z_window_spin.setValue(14)
        self.z_window_spin.valueChanged.connect(self.config_changed.emit)
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Dark", "Light"])
        self.theme_combo.currentTextChanged.connect(self.theme_changed.emit)
        self.text_size_spin = QSpinBox()
        self.text_size_spin.setRange(8, 14)
        self.text_size_spin.setValue(12)
        self.text_size_spin.valueChanged.connect(self.config_changed.emit)

        display_box = QGroupBox("Display")
        display_form = QFormLayout(display_box)
        display_form.addRow("Compare Date", self.compare_date_combo)
        display_form.addRow("Y Axis", self.y_axis_mode_combo)
        display_form.addRow("Live Price", self.live_price_mode_combo)
        display_form.addRow("Z Window", self.z_window_spin)
        display_form.addRow("Text Size", self.text_size_spin)
        display_form.addRow("Theme", self.theme_combo)
        root.addWidget(display_box)

        button_row = QHBoxLayout()
        load_history_btn = QPushButton("Load API History")
        reset_layout_btn = QPushButton("Reset Grid")
        self.focus_mode_btn = QPushButton("Focus Charts")
        load_history_btn.clicked.connect(self.load_api_history_requested.emit)
        reset_layout_btn.clicked.connect(self.reset_layout_requested.emit)
        self.focus_mode_btn.clicked.connect(self.focus_mode_requested.emit)
        button_row.addWidget(load_history_btn)
        button_row.addWidget(reset_layout_btn)
        button_row.addWidget(self.focus_mode_btn)
        root.addLayout(button_row)
        root.addStretch(1)

    def set_instruments(self, instruments: list[str]) -> None:
        selected = set(self.selected_instruments())
        self.instrument_list.blockSignals(True)
        self.instrument_list.clear()
        for instrument in instruments:
            item = QListWidgetItem(instrument)
            self.instrument_list.addItem(item)
            item.setSelected(instrument in selected or not selected)
        self.instrument_list.blockSignals(False)
        if instruments and not selected:
            for idx in range(self.instrument_list.count()):
                self.instrument_list.item(idx).setSelected(True)

    def selected_instruments(self) -> list[str]:
        return [item.text() for item in self.instrument_list.selectedItems()]

    def set_history_dates(self, dates: list[str]) -> None:
        if dates == self._history_dates:
            return
        current = self.compare_date_combo.currentText()
        self._history_dates = list(dates)
        self.compare_date_combo.blockSignals(True)
        self.compare_date_combo.clear()
        self.compare_date_combo.addItem("Latest vs latest history")
        for date_text in reversed(dates):
            self.compare_date_combo.addItem(date_text)
        idx = self.compare_date_combo.findText(current)
        self.compare_date_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.compare_date_combo.blockSignals(False)

    def compare_date(self) -> str | None:
        text = self.compare_date_combo.currentText().strip()
        return None if not text or text == "Latest vs latest history" else text

    def config(self) -> dict:
        return {
            "selected_instruments": self.selected_instruments(),
            "compare_date": self.compare_date(),
            "y_axis_mode": self.y_axis_mode_combo.currentText().strip().lower(),
            "live_price_mode": "vwap" if self.live_price_mode_combo.currentText().strip().lower() == "vwap" else "last",
            "z_window": int(self.z_window_spin.value()),
            "text_size": int(self.text_size_spin.value()),
            "theme": self.theme_combo.currentText().strip().lower(),
        }

    def apply_config(self, config: dict) -> None:
        if not isinstance(config, dict):
            return
        selected = set(str(item) for item in config.get("selected_instruments", []))
        self.instrument_list.blockSignals(True)
        for idx in range(self.instrument_list.count()):
            item = self.instrument_list.item(idx)
            item.setSelected(item.text() in selected)
        self.instrument_list.blockSignals(False)

        compare_date = str(config.get("compare_date", "") or "")
        idx = self.compare_date_combo.findText(compare_date)
        self.compare_date_combo.setCurrentIndex(idx if idx >= 0 else 0)

        y_axis_mode = "Change" if str(config.get("y_axis_mode", "actual")).strip().lower() == "change" else "Actual"
        self.y_axis_mode_combo.setCurrentText(y_axis_mode)
        live_mode = "VWAP" if str(config.get("live_price_mode", "last")).strip().lower() == "vwap" else "Last"
        self.live_price_mode_combo.setCurrentText(live_mode)
        self.z_window_spin.setValue(int(config.get("z_window", 14)))
        self.text_size_spin.setValue(int(config.get("text_size", 12)))
        theme_name = "Light" if str(config.get("theme", "dark")).strip().lower() == "light" else "Dark"
        self.theme_combo.setCurrentText(theme_name)

    def set_focus_mode(self, enabled: bool) -> None:
        self.focus_mode_btn.setText("Exit Focus" if enabled else "Focus Charts")


class StatsPanel(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self.status_label = QLabel("Feed disconnected")
        self.status_label.setObjectName("statsHeadline")
        self.compare_label = QLabel("Compare date: n/a")
        self.mode_label = QLabel("Axis mode: actual")
        self.hover_label = QLabel("Hover: n/a")
        self.hover_label.setWordWrap(True)
        self.selection_label = QLabel("Selection: n/a")
        self.z_extreme_label = QLabel("Max |z|: n/a")
        self.history_label = QLabel("History rows: 0")

        for widget in [
            self.status_label,
            self.compare_label,
            self.mode_label,
            self.selection_label,
            self.hover_label,
            self.z_extreme_label,
            self.history_label,
        ]:
            layout.addWidget(widget)

        layout.addWidget(QLabel("Spread Radar"))
        self.summary_table = QTableWidget(0, 3)
        self.summary_table.setHorizontalHeaderLabels(["Bucket", "Latest", "Change"])
        self.summary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.summary_table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        layout.addWidget(self.summary_table)

        layout.addWidget(QLabel("Notes"))
        self.notes_box = QTextEdit()
        self.notes_box.setReadOnly(True)
        layout.addWidget(self.notes_box, 1)

    def update_status(self, status: str, last_tick: str | None = None) -> None:
        suffix = f" | Last tick {last_tick}" if last_tick else ""
        self.status_label.setText(f"{status}{suffix}")

    def update_context(self, compare_date: str | None, y_axis_mode: str, selected_count: int, history_rows: int) -> None:
        self.compare_label.setText(f"Compare date: {compare_date or 'latest history'}")
        self.mode_label.setText(f"Axis mode: {y_axis_mode}")
        self.selection_label.setText(f"Selection: {selected_count} contracts")
        self.history_label.setText(f"History rows: {history_rows}")

    def update_hover(self, title: str | None, x_label: str | None, value: float | None) -> None:
        if not title:
            self.hover_label.setText("Hover: n/a")
            return
        value_text = "n/a" if value is None else f"{value:.4f}"
        self.hover_label.setText(f"Hover: {title} | {x_label or 'n/a'} | {value_text}")

    def update_summary_rows(self, rows: list[dict[str, str]], z_extreme_text: str, notes: str) -> None:
        self.summary_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            self.summary_table.setItem(row_idx, 0, QTableWidgetItem(row.get("bucket", "")))
            self.summary_table.setItem(row_idx, 1, QTableWidgetItem(row.get("latest", "")))
            self.summary_table.setItem(row_idx, 2, QTableWidgetItem(row.get("change", "")))
        self.z_extreme_label.setText(z_extreme_text)
        self.notes_box.setPlainText(notes)
