import argparse
import os
import sys
import logging
from pathlib import Path
import yfinance as yf
import pandas as pd
import pandas_ta as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def safe_ticker_path(ticker: str) -> str:
    """Convert ticker to filesystem-safe name."""
    return ticker.replace('^', '_').replace('/', '_')


def download_and_compute(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Download OHLCV data for ticker and compute technical indicators."""
    logger.info(f"Downloading {ticker} from {start} to {end}")
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)

    if df.empty:
        raise ValueError(f"No data returned for {ticker}")

    # Flatten MultiIndex columns if present (yfinance sometimes returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Rename columns to lowercase
    df.columns = [c.lower() for c in df.columns]

    # Add ticker column
    df['ticker'] = ticker

    # Reset index so date becomes a column
    df = df.reset_index()
    df = df.rename(columns={'index': 'date', 'Date': 'date'})
    if 'date' not in df.columns and 'Date' in df.columns:
        df = df.rename(columns={'Date': 'date'})

    # Ensure date column is named correctly
    df.columns = [c.lower() for c in df.columns]

    # yfinance with auto_adjust=True: adjusted close is in 'close', no separate 'adj close'
    # We'll store adj_close as a copy of close (since auto_adjust=True means close IS adj close)
    if 'adj_close' not in df.columns and 'adj close' not in df.columns:
        df['adj_close'] = df['close']
    elif 'adj close' in df.columns:
        df = df.rename(columns={'adj close': 'adj_close'})

    # Compute indicators using pandas-ta
    # Set the index back to date for pandas-ta compatibility
    df = df.set_index('date')

    # SMA
    df.ta.sma(length=20, append=True)
    df.ta.sma(length=50, append=True)
    df.ta.sma(length=200, append=True)

    # EMA
    df.ta.ema(length=12, append=True)
    df.ta.ema(length=26, append=True)

    # RSI
    df.ta.rsi(length=14, append=True)

    # MACD: produces MACDh_12_26_9, MACDs_12_26_9, MACD_12_26_9
    df.ta.macd(fast=12, slow=26, signal=9, append=True)

    # Bollinger Bands: produces BBU_20_2.0, BBM_20_2.0, BBL_20_2.0, BBB_20_2.0, BBP_20_2.0
    df.ta.bbands(length=20, append=True)

    # ATR: produces ATRr_14
    df.ta.atr(length=14, append=True)

    # OBV: produces OBV
    df.ta.obv(append=True)

    # Reset index so date is a column again
    df = df.reset_index()

    # Rename all columns to lowercase first
    df.columns = [c.lower() for c in df.columns]

    # Rename pandas-ta output columns to canonical names
    rename_map = {}

    # SMA columns: SMA_20, SMA_50, SMA_200 -> sma_20, sma_50, sma_200 (already lowercased)
    for col in df.columns:
        col_lower = col.lower()
        if col_lower == 'sma_20':
            rename_map[col] = 'sma_20'
        elif col_lower == 'sma_50':
            rename_map[col] = 'sma_50'
        elif col_lower == 'sma_200':
            rename_map[col] = 'sma_200'
        elif col_lower == 'ema_12':
            rename_map[col] = 'ema_12'
        elif col_lower == 'ema_26':
            rename_map[col] = 'ema_26'
        elif col_lower == 'rsi_14':
            rename_map[col] = 'rsi_14'
        # MACD columns: macdh_12_26_9 -> macd_hist, macds_12_26_9 -> macd_signal, macd_12_26_9 -> macd
        elif col_lower == 'macdh_12_26_9':
            rename_map[col] = 'macd_hist'
        elif col_lower == 'macds_12_26_9':
            rename_map[col] = 'macd_signal'
        elif col_lower == 'macd_12_26_9':
            rename_map[col] = 'macd'
        # Bollinger Bands: bbu_20_2.0 -> bb_upper, bbm_20_2.0 -> bb_middle, etc.
        elif col_lower == 'bbu_20_2.0':
            rename_map[col] = 'bb_upper'
        elif col_lower == 'bbm_20_2.0':
            rename_map[col] = 'bb_middle'
        elif col_lower == 'bbl_20_2.0':
            rename_map[col] = 'bb_lower'
        elif col_lower == 'bbb_20_2.0':
            rename_map[col] = 'bb_bandwidth'
        elif col_lower == 'bbp_20_2.0':
            rename_map[col] = 'bb_pct_b'
        # ATR: atrr_14 -> atr_14
        elif col_lower == 'atrr_14':
            rename_map[col] = 'atr_14'
        # OBV: obv -> obv (already lowercase, no change needed unless different case)

    df = df.rename(columns=rename_map)

    # Ensure all columns are lowercase
    df.columns = [c.lower() for c in df.columns]

    # Select and reorder final columns
    final_columns = [
        'date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'adj_close',
        'sma_20', 'sma_50', 'sma_200', 'ema_12', 'ema_26', 'rsi_14',
        'macd', 'macd_signal', 'macd_hist',
        'bb_upper', 'bb_middle', 'bb_lower', 'bb_bandwidth', 'bb_pct_b',
        'atr_14', 'obv',
    ]

    # Only keep columns that exist (some may be missing if data is too short)
    existing_columns = [c for c in final_columns if c in df.columns]
    missing = [c for c in final_columns if c not in df.columns]
    if missing:
        logger.warning(f"Missing columns for {ticker}: {missing}")

    df = df[existing_columns]

    return df


def save_parquet(df: pd.DataFrame, output_dir: Path, ticker: str, force: bool) -> None:
    """Save dataframe split by year as Parquet files."""
    ticker_path = safe_ticker_path(ticker)
    ticker_dir = output_dir / ticker_path
    ticker_dir.mkdir(parents=True, exist_ok=True)

    # Group by year
    df['year'] = pd.to_datetime(df['date']).dt.year

    for year, year_df in df.groupby('year'):
        out_path = ticker_dir / f"{year}.parquet"

        if out_path.exists() and not force:
            logger.info(f"Skipping existing file: {out_path}")
            continue

        # Drop the helper year column before saving
        year_df = year_df.drop(columns=['year'])
        year_df.to_parquet(out_path, index=False)
        logger.info(f"Saved {len(year_df)} rows to {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Download OHLCV data and pre-compute technical indicators, saving to Parquet.'
    )
    parser.add_argument(
        '--tickers',
        nargs='+',
        required=True,
        help='List of ticker symbols (e.g. SPY AAPL ^VIX)',
    )
    parser.add_argument(
        '--start',
        required=True,
        help='Start date in YYYY-MM-DD format',
    )
    parser.add_argument(
        '--end',
        required=True,
        help='End date in YYYY-MM-DD format',
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Output directory for Parquet files',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        default=False,
        help='Overwrite existing Parquet files',
    )

    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    failed = []

    for ticker in args.tickers:
        try:
            df = download_and_compute(ticker, args.start, args.end)
            save_parquet(df, output_dir, ticker, args.force)
            logger.info(f"Completed {ticker}")
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}")
            failed.append(ticker)

    if failed:
        logger.error(f"Failed tickers: {failed}")
        sys.exit(1)


if __name__ == '__main__':
    main()
