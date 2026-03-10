import argparse
import os
import sys
import logging
from datetime import datetime
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
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)

    if df.empty:
        raise ValueError(f"No data returned for {ticker}")

    # Flatten MultiIndex columns if present (yfinance sometimes returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Single lowercasing pass — all subsequent checks use lowercase names
    df.columns = [c.lower() for c in df.columns]

    # yfinance with auto_adjust=False: 'Adj Close' → lowercased to 'adj close'
    # Rename to underscore form immediately after lowercasing
    if 'adj close' in df.columns:
        df = df.rename(columns={'adj close': 'adj_close'})

    if 'adj_close' not in df.columns:
        raise RuntimeError(
            f"adj_close column not found for {ticker} after lowercasing. "
            "Expected 'Adj Close' from yfinance with auto_adjust=False."
        )

    # Add ticker column
    df['ticker'] = ticker

    # Reset index so date becomes a column
    df = df.reset_index()
    df = df.rename(columns={'index': 'date', 'date': 'date'})

    # Compute indicators using pandas-ta
    # Set the index back to date for pandas-ta compatibility
    df = df.set_index('date')

    # NOTE: Warm-up NaN rows — the first N rows will have NaN for indicators
    # that require look-back periods:
    #   - SMA-20:  first 19 rows are NaN
    #   - SMA-50:  first 49 rows are NaN
    #   - SMA-200: first 199 rows are NaN  ← longest warm-up
    #   - RSI-14:  first 14 rows are NaN
    #   - MACD(12,26,9): first ~33 rows are NaN
    # If you need fully-populated indicator values from a specific date, pass
    # --start at least 200 trading days (~10 months) before that date.

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

    # Lowercase all columns once after indicator computation
    df.columns = [c.lower() for c in df.columns]

    # Rename pandas-ta output columns to canonical names.
    # Only columns that need renaming are listed — identity renames are omitted.
    rename_mapping = {
        'macdh_12_26_9': 'macd_hist',
        'macds_12_26_9': 'macd_signal',
        'macd_12_26_9': 'macd',
        'bbu_20_2.0': 'bb_upper',
        'bbm_20_2.0': 'bb_middle',
        'bbl_20_2.0': 'bb_lower',
        'bbb_20_2.0': 'bb_bandwidth',
        'bbp_20_2.0': 'bb_pct_b',
        'atrr_14': 'atr_14',  # pandas_ta Wilder ATR variant
        'atr_14': 'atr_14',   # pandas_ta simple ATR variant (no-op, explicit for clarity)
    }
    df = df.rename(columns=rename_mapping)

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

    # Validate --start and --end date formats and ordering
    try:
        start_dt = datetime.strptime(args.start, '%Y-%m-%d')
    except ValueError:
        parser.error(f"--start must be in YYYY-MM-DD format, got: {args.start!r}")
    try:
        end_dt = datetime.strptime(args.end, '%Y-%m-%d')
    except ValueError:
        parser.error(f"--end must be in YYYY-MM-DD format, got: {args.end!r}")
    if start_dt >= end_dt:
        parser.error(f"--start ({args.start}) must be before --end ({args.end})")

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
