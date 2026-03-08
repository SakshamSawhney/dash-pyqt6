from __future__ import annotations

from pathlib import Path

import pandas as pd


def _parse_excel_timestamp(series: pd.Series) -> pd.Series:
    as_text = series.astype(str).str.strip()
    parsed_ddmmyyyy = pd.to_datetime(as_text, format='%d-%m-%Y', utc=True, errors='coerce')
    parsed_text = pd.to_datetime(series, utc=True, errors='coerce', dayfirst=True)

    numeric = pd.to_numeric(series, errors='coerce')
    parsed_excel = pd.to_datetime(numeric, unit='D', origin='1899-12-30', utc=True, errors='coerce')

    out = parsed_ddmmyyyy.where(parsed_ddmmyyyy.notna(), parsed_text)
    out = out.where(out.notna(), parsed_excel)
    return out


def _standardize_long_format(df: pd.DataFrame) -> pd.DataFrame | None:
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    required = {'timestamp', 'instrument', 'price'}
    if not required.issubset(lower_map.keys()):
        return None

    out = df[[lower_map['timestamp'], lower_map['instrument'], lower_map['price']]].copy()
    out.columns = ['timestamp', 'instrument', 'price']
    return out


def _standardize_wide_format(df: pd.DataFrame) -> pd.DataFrame | None:
    if df.shape[1] < 2:
        return None

    ts_col = df.columns[0]
    out = df.copy()
    out[ts_col] = _parse_excel_timestamp(out[ts_col])
    out = out.dropna(subset=[ts_col])
    if out.empty:
        return None

    melted = out.melt(id_vars=[ts_col], var_name='instrument', value_name='price')
    melted = melted.rename(columns={ts_col: 'timestamp'})
    return melted


def _read_excel_sheet(path: Path, preferred_sheet: str = 'LOIS') -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine='openpyxl')
    sheet_names = xls.sheet_names

    target_sheet = preferred_sheet if preferred_sheet in sheet_names else sheet_names[0]
    return pd.read_excel(path, sheet_name=target_sheet, engine='openpyxl')


def load_historical_excel(path: Path, sheet_name: str = 'LOIS') -> pd.DataFrame:
    raw = _read_excel_sheet(path, preferred_sheet=sheet_name)

    df = _standardize_long_format(raw)
    if df is None:
        df = _standardize_wide_format(raw)
    if df is None:
        raise ValueError('Unable to parse historical Excel. Expected long format or wide time-series format.')

    df['timestamp'] = _parse_excel_timestamp(df['timestamp'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['instrument'] = df['instrument'].astype(str).str.strip()
    df = df.dropna(subset=['timestamp', 'instrument', 'price'])

    df['bid'] = df['price']
    df['ask'] = df['price']
    df['volume'] = 0.0
    return df[['timestamp', 'instrument', 'price', 'bid', 'ask', 'volume']].sort_values('timestamp').reset_index(drop=True)