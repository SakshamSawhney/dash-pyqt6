from __future__ import annotations

from typing import Sequence

import pandas as pd
import pyqtgraph as pg


class DateIndexAxisItem(pg.AxisItem):
    """Position axis that shows date labels derived from row index."""

    def __init__(self, orientation: str = 'bottom') -> None:
        super().__init__(orientation=orientation)
        self._labels: dict[int, str] = {}

    def set_index_labels(self, index: Sequence) -> None:
        self._labels.clear()
        if not index:
            return

        dt = pd.to_datetime(pd.Index(index), errors='coerce', dayfirst=True, utc=True)
        n = len(dt)
        for i in range(n):
            if pd.isna(dt[i]):
                self._labels[i] = str(index[i])
            else:
                self._labels[i] = dt[i].strftime('%Y-%m-%d')

    def label_for_index(self, idx: int) -> str:
        return self._labels.get(idx, '')

    def tickStrings(self, values, scale, spacing):  # noqa: N802
        labels = []
        for v in values:
            key = int(round(v))
            labels.append(self._labels.get(key, ''))
        return labels