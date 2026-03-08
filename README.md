# Rates Dashboard

Professional PyQt6 dashboard for Euribor futures / LOIS analytics with live + historical data.

## Features
- Live market data via your Lightstreamer feed (no simulated ticks)
- Historical Excel loader (supports long format and wide time-series format)
- Unified in-memory Pandas store (`timestamp | instrument | price | bid | ask | volume`)
- Spread and fly builders
- Rolling z-score
- Correlation matrix
- OLS regression (statsmodels)
- Yield-curve chart by contract maturity
- High-performance PyQtGraph realtime chart
- Periodic Parquet persistence (no per-tick disk writes)

## Install
```bash
pip install -r requirements.txt
```

## Run
```bash
python main.py --historical-file "C:/Users/saksh/Downloads/LOIS 12th Feb.xlsx"
```

## Lightstreamer Configuration
Configured in `data/lightstreamer_client.py` with:
- Server URL: `https://ls-md.corp.hertshtengroup.com/`
- Adapter set: `paste`
- Data adapter: `paste`
- Your subscribed IDs and field list

The stream thread maps updates into dashboard ticks:
- `timestamp` from `ClientRecvTime` / `ExchangeRecvTime` / `ServerRecvTime`
- `instrument` from `Contract` (fallback: `ItemName`)
- `price` from `Price` (fallbacks: `Last`, `Close`)
- `bid` from `BestBid`
- `ask` from `BestAsk`
- `volume` from `Volume`