from __future__ import annotations

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox,
    QDoubleSpinBox,
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


class LeftControlPanel(QWidget):
    config_changed = pyqtSignal()

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)

        self.instrument_list = QListWidget()
        self.instrument_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.instrument_list.itemSelectionChanged.connect(self.config_changed.emit)

        instrument_box = QGroupBox('Instruments (Main Chart)')
        instrument_layout = QVBoxLayout(instrument_box)
        instrument_layout.addWidget(self.instrument_list)

        self.spread_c1 = QComboBox()
        self.spread_c2 = QComboBox()
        self.spread_add_btn = QPushButton('Add Spread')
        self.spread_remove_btn = QPushButton('Delete Spread')
        self.spread_list = QListWidget()
        self.spread_list.itemChanged.connect(self.config_changed.emit)
        self.spread_add_btn.clicked.connect(self._add_spread)
        self.spread_remove_btn.clicked.connect(self._remove_spread)

        spread_box = QGroupBox('Spread Builder (Uncheck to hide line)')
        spread_layout = QVBoxLayout(spread_box)
        spread_form = QFormLayout()
        spread_form.addRow('Contract 1', self.spread_c1)
        spread_form.addRow('Contract 2', self.spread_c2)
        spread_layout.addLayout(spread_form)
        spread_btns = QHBoxLayout()
        spread_btns.addWidget(self.spread_add_btn)
        spread_btns.addWidget(self.spread_remove_btn)
        spread_layout.addLayout(spread_btns)
        spread_layout.addWidget(self.spread_list)

        self.fly_c1 = QComboBox()
        self.fly_c2 = QComboBox()
        self.fly_c3 = QComboBox()
        self.fly_add_btn = QPushButton('Add Fly')
        self.fly_remove_btn = QPushButton('Delete Fly')
        self.fly_list = QListWidget()
        self.fly_list.itemChanged.connect(self.config_changed.emit)
        self.fly_add_btn.clicked.connect(self._add_fly)
        self.fly_remove_btn.clicked.connect(self._remove_fly)

        fly_box = QGroupBox('Fly Builder (Uncheck to hide line)')
        fly_layout = QVBoxLayout(fly_box)
        fly_form = QFormLayout()
        fly_form.addRow('Contract 1', self.fly_c1)
        fly_form.addRow('Contract 2', self.fly_c2)
        fly_form.addRow('Contract 3', self.fly_c3)
        fly_layout.addLayout(fly_form)
        fly_btns = QHBoxLayout()
        fly_btns.addWidget(self.fly_add_btn)
        fly_btns.addWidget(self.fly_remove_btn)
        fly_layout.addLayout(fly_btns)
        fly_layout.addWidget(self.fly_list)

        self.curve_instrument_list = QListWidget()
        self.curve_instrument_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.curve_instrument_list.itemSelectionChanged.connect(self.config_changed.emit)
        curve_box = QGroupBox('Yield Curve Instruments')
        curve_layout = QVBoxLayout(curve_box)
        curve_layout.addWidget(self.curve_instrument_list)

        self.z_window = QSpinBox()
        self.z_window.setRange(20, 2000)
        self.z_window.setValue(200)
        self.z_window.valueChanged.connect(self.config_changed.emit)

        self.sigma_level = QDoubleSpinBox()
        self.sigma_level.setRange(0.5, 10.0)
        self.sigma_level.setSingleStep(0.25)
        self.sigma_level.setDecimals(2)
        self.sigma_level.setValue(2.0)
        self.sigma_level.valueChanged.connect(self.config_changed.emit)

        analytics_box = QGroupBox('Analytics Params')
        analytics_layout = QFormLayout(analytics_box)
        analytics_layout.addRow('Z-score Window', self.z_window)
        analytics_layout.addRow('Sigma Level (+/-)', self.sigma_level)

        root.addWidget(instrument_box)
        root.addWidget(spread_box)
        root.addWidget(fly_box)
        root.addWidget(curve_box)
        root.addWidget(analytics_box)
        root.addStretch(1)

    @staticmethod
    def _add_formula_item(list_widget: QListWidget, label: str, payload: list[str], enabled: bool = True) -> None:
        item = QListWidgetItem(label)
        item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(Qt.CheckState.Checked if enabled else Qt.CheckState.Unchecked)
        item.setData(Qt.ItemDataRole.UserRole, payload)
        list_widget.addItem(item)

    @staticmethod
    def _list_entries(list_widget: QListWidget) -> list[dict]:
        out: list[dict] = []
        for idx in range(list_widget.count()):
            item = list_widget.item(idx)
            payload = item.data(Qt.ItemDataRole.UserRole)
            if isinstance(payload, list):
                out.append(
                    {
                        'legs': [str(x) for x in payload],
                        'enabled': item.checkState() == Qt.CheckState.Checked,
                    }
                )
        return out

    @staticmethod
    def _payload_exists(list_widget: QListWidget, payload: list[str]) -> bool:
        for idx in range(list_widget.count()):
            existing = list_widget.item(idx).data(Qt.ItemDataRole.UserRole)
            if isinstance(existing, list) and [str(x) for x in existing] == payload:
                return True
        return False

    def _add_spread(self) -> None:
        c1 = self.spread_c1.currentText().strip()
        c2 = self.spread_c2.currentText().strip()
        if not c1 or not c2:
            return
        payload = [c1, c2]
        if not self._payload_exists(self.spread_list, payload):
            self._add_formula_item(self.spread_list, f'{c1}-{c2}', payload, enabled=True)
            self.config_changed.emit()

    def _remove_spread(self) -> None:
        for item in self.spread_list.selectedItems():
            self.spread_list.takeItem(self.spread_list.row(item))
        self.config_changed.emit()

    def _add_fly(self) -> None:
        c1 = self.fly_c1.currentText().strip()
        c2 = self.fly_c2.currentText().strip()
        c3 = self.fly_c3.currentText().strip()
        if not c1 or not c2 or not c3:
            return
        payload = [c1, c2, c3]
        if not self._payload_exists(self.fly_list, payload):
            self._add_formula_item(self.fly_list, f'{c1}-2*{c2}+{c3}', payload, enabled=True)
            self.config_changed.emit()

    def _remove_fly(self) -> None:
        for item in self.fly_list.selectedItems():
            self.fly_list.takeItem(self.fly_list.row(item))
        self.config_changed.emit()

    def set_instruments(self, instruments: list[str]) -> None:
        selected_main = set(self.selected_instruments())
        selected_curve = set(self.selected_curve_instruments())

        spreads = self._list_entries(self.spread_list)
        flies = self._list_entries(self.fly_list)
        valid = set(instruments)

        self.instrument_list.blockSignals(True)
        self.curve_instrument_list.blockSignals(True)
        self.instrument_list.clear()
        self.curve_instrument_list.clear()
        for ins in instruments:
            item_main = QListWidgetItem(ins)
            if ins in selected_main:
                item_main.setSelected(True)
            self.instrument_list.addItem(item_main)

            item_curve = QListWidgetItem(ins)
            if ins in selected_curve:
                item_curve.setSelected(True)
            self.curve_instrument_list.addItem(item_curve)
        self.instrument_list.blockSignals(False)
        self.curve_instrument_list.blockSignals(False)

        for combo in [self.spread_c1, self.spread_c2, self.fly_c1, self.fly_c2, self.fly_c3]:
            current = combo.currentText()
            combo.blockSignals(True)
            combo.clear()
            combo.addItems(instruments)
            if current and current in instruments:
                combo.setCurrentText(current)
            combo.blockSignals(False)

        self.spread_list.blockSignals(True)
        self.fly_list.blockSignals(True)
        self.spread_list.clear()
        for entry in spreads:
            legs = entry.get('legs', [])
            enabled = bool(entry.get('enabled', True))
            if isinstance(legs, list) and len(legs) == 2 and all(x in valid for x in legs):
                self._add_formula_item(self.spread_list, f'{legs[0]}-{legs[1]}', legs, enabled=enabled)

        self.fly_list.clear()
        for entry in flies:
            legs = entry.get('legs', [])
            enabled = bool(entry.get('enabled', True))
            if isinstance(legs, list) and len(legs) == 3 and all(x in valid for x in legs):
                self._add_formula_item(self.fly_list, f'{legs[0]}-2*{legs[1]}+{legs[2]}', legs, enabled=enabled)
        self.spread_list.blockSignals(False)
        self.fly_list.blockSignals(False)

        self.config_changed.emit()

    def selected_instruments(self) -> list[str]:
        return [item.text() for item in self.instrument_list.selectedItems()]

    def selected_curve_instruments(self) -> list[str]:
        return [item.text() for item in self.curve_instrument_list.selectedItems()]

    def set_config(self, config: dict) -> None:
        selected = set(config.get('selected_instruments', []))
        for i in range(self.instrument_list.count()):
            item = self.instrument_list.item(i)
            item.setSelected(item.text() in selected)

        curve_selected = set(config.get('yield_curve_instruments', []))
        for i in range(self.curve_instrument_list.count()):
            item = self.curve_instrument_list.item(i)
            item.setSelected(item.text() in curve_selected)

        self.z_window.setValue(int(config.get('z_window', 200)))
        self.sigma_level.setValue(float(config.get('sigma_level', 2.0)))

        self.spread_list.blockSignals(True)
        self.fly_list.blockSignals(True)

        self.spread_list.clear()
        for entry in config.get('spreads', []):
            if isinstance(entry, dict):
                legs = entry.get('legs', [])
                enabled = bool(entry.get('enabled', True))
            else:
                legs = entry
                enabled = True
            if isinstance(legs, list) and len(legs) == 2:
                self._add_formula_item(self.spread_list, f'{legs[0]}-{legs[1]}', [str(legs[0]), str(legs[1])], enabled)

        self.fly_list.clear()
        for entry in config.get('flies', []):
            if isinstance(entry, dict):
                legs = entry.get('legs', [])
                enabled = bool(entry.get('enabled', True))
            else:
                legs = entry
                enabled = True
            if isinstance(legs, list) and len(legs) == 3:
                self._add_formula_item(
                    self.fly_list,
                    f'{legs[0]}-2*{legs[1]}+{legs[2]}',
                    [str(legs[0]), str(legs[1]), str(legs[2])],
                    enabled,
                )

        self.spread_list.blockSignals(False)
        self.fly_list.blockSignals(False)

        self.config_changed.emit()

    def get_config(self) -> dict:
        return {
            'selected_instruments': self.selected_instruments(),
            'spreads': self._list_entries(self.spread_list),
            'flies': self._list_entries(self.fly_list),
            'yield_curve_instruments': self.selected_curve_instruments(),
            'z_window': int(self.z_window.value()),
            'sigma_level': float(self.sigma_level.value()),
        }


class StatsPanel(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)

        self.live_status_label = QLabel('Live: disconnected')
        self.last_tick_label = QLabel('Last tick: n/a')
        self.zscore_label = QLabel('Z-score: n/a')

        self.regression_box = QTextEdit()
        self.regression_box.setReadOnly(True)
        self.correlation_box = QTextEdit()
        self.correlation_box.setReadOnly(True)

        layout.addWidget(self.live_status_label)
        layout.addWidget(self.last_tick_label)
        layout.addWidget(self.zscore_label)
        layout.addWidget(QLabel('Correlation Matrix'))
        layout.addWidget(self.correlation_box)
        layout.addWidget(QLabel('Regression (LOIS ~ Spread)'))
        layout.addWidget(self.regression_box)

    def update_live(self, status: str, last_tick: str | None = None) -> None:
        self.live_status_label.setText(f'Live: {status}')
        if last_tick:
            self.last_tick_label.setText(f'Last tick: {last_tick}')

    def update_stats(self, zscore: float | None, corr_text: str, regression_text: str) -> None:
        if zscore is None:
            self.zscore_label.setText('Z-score: n/a')
        else:
            self.zscore_label.setText(f'Z-score: {zscore:.3f}')

        self.correlation_box.setPlainText(corr_text)
        self.regression_box.setPlainText(regression_text)


class BottomTablePanel(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(['timestamp', 'instrument', 'price', 'bid', 'ask', 'volume'])
        layout.addWidget(self.table)

    def update_rows(self, rows) -> None:
        self.table.setRowCount(len(rows))
        for r_idx, row in enumerate(rows):
            for c_idx, value in enumerate(row):
                self.table.setItem(r_idx, c_idx, QTableWidgetItem(str(value)))