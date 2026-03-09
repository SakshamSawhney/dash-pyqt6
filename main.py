from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PyQt6.QtWidgets import QApplication

from data.data_store import MarketDataStore
from data.historical_loader import load_historical_excel
from ui.main_window import MainWindow

DEFAULT_HISTORICAL_FILE = Path(__file__).resolve().parent / 'LOIS 12th Feb.xlsx'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Interest Rate Derivatives Dashboard')
    parser.add_argument(
        '--historical-file',
        type=str,
        default=str(DEFAULT_HISTORICAL_FILE),
        help='Historical Excel source',
    )
    parser.add_argument(
        '--historical-sheet',
        type=str,
        default='LOIS',
        help='Worksheet name for historical data (default: LOIS)',
    )
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
    historical_path = Path(args.historical_file)
    if historical_path.exists():
        hist_df = load_historical_excel(historical_path, sheet_name=args.historical_sheet)
        data_store.load_historical(hist_df)

    window = MainWindow(
        data_store=data_store,
        persist_path=Path(args.persist_path),
    )
    window.show()
    return app.exec()


if __name__ == '__main__':
    raise SystemExit(main())
