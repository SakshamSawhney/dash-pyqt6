from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication

from data.data_store import MarketDataStore
from ui.main_window import MainWindow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Interest Rate Derivatives Dashboard')
    parser.add_argument(
        '--persist-path',
        type=str,
        default='market_data.parquet',
        help='Parquet path for periodic persistence',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = QApplication(sys.argv)

    data_store = MarketDataStore()

    window = MainWindow(
        data_store=data_store,
        persist_path=Path(args.persist_path),
    )
    window.show()
    return app.exec()


if __name__ == '__main__':
    raise SystemExit(main())
