"""
Black-Scholes greeks calculator for options chain Parquet files (EP 4).

Reads raw options chain Parquet files produced by EP 3 (alpaca_options_backfill.py)
and adds five Black-Scholes greek columns: delta, gamma, theta, vega, rho.

Usage:
    python greeks_calculator.py --input-dir data/options/ --output-dir data/options_with_greeks/ --risk-free-rate 0.05
    python greeks_calculator.py --input-dir data/options/ --output-dir data/options_with_greeks/ --ohlcv-dir data/ohlcv/ --force
"""
import argparse
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# py_vollib import (optional — gracefully degrade if not installed)
# ---------------------------------------------------------------------------
try:
    from py_vollib.black_scholes.greeks.analytical import (
        delta, gamma, theta, vega, rho
    )
    PY_VOLLIB_AVAILABLE = True
except ImportError:
    PY_VOLLIB_AVAILABLE = False
    logger.warning(
        "py_vollib is not installed. Greeks will be set to NaN. "
        "Install with: pip install py_vollib"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_NAN_GREEKS = {
    'delta': float('nan'),
    'gamma': float('nan'),
    'theta': float('nan'),
    'vega': float('nan'),
    'rho': float('nan'),
}


# ---------------------------------------------------------------------------
# Helper: filesystem-safe ticker name (mirrors yfinance_backfill.py)
# ---------------------------------------------------------------------------

def safe_ticker_path(ticker: str) -> str:
    """Convert ticker to filesystem-safe name (e.g. ^VIX → _VIX)."""
    return ticker.replace('^', '_').replace('/', '_')


# ---------------------------------------------------------------------------
# Greeks computation
# ---------------------------------------------------------------------------

def compute_greeks_row(flag: str, S: float, K: float, t: float, r: float, sigma: float) -> dict:
    """
    Compute all Black-Scholes greeks for one option contract.

    Parameters
    ----------
    flag  : 'c' for call, 'p' for put
    S     : spot price of the underlying
    K     : strike price
    t     : time to expiration in years (days_to_expiry / 365.0)
    r     : risk-free interest rate (e.g. 0.05)
    sigma : implied volatility (e.g. 0.20)

    Returns a dict with keys delta, gamma, theta, vega, rho.
    All values are NaN on any error or invalid inputs.
    """
    # Guard: invalid inputs → NaN
    if not PY_VOLLIB_AVAILABLE:
        return dict(_NAN_GREEKS)
    if (
        sigma is None or np.isnan(sigma) or sigma <= 0
        or t is None or np.isnan(t) or t <= 0
        or K is None or np.isnan(K) or K <= 0
        or S is None or np.isnan(S) or S <= 0
    ):
        return dict(_NAN_GREEKS)

    try:
        return {
            'delta': delta(flag, S, K, t, r, sigma),
            'gamma': gamma(flag, S, K, t, r, sigma),
            'theta': theta(flag, S, K, t, r, sigma),
            'vega': vega(flag, S, K, t, r, sigma),
            'rho': rho(flag, S, K, t, r, sigma),
        }
    except Exception as exc:
        logger.debug(f"py_vollib error for flag={flag} S={S} K={K} t={t} sigma={sigma}: {exc}")
        return dict(_NAN_GREEKS)


# ---------------------------------------------------------------------------
# OHLCV spot price lookup
# ---------------------------------------------------------------------------

def get_spot_price(underlying: str, trade_date: date, ohlcv_dir: Path) -> float | None:
    """
    Look up the closing price of *underlying* on *trade_date* from OHLCV parquets.

    OHLCV files are stored as {ohlcv_dir}/{safe_ticker}/{year}.parquet.
    The 'close' column is used as the spot price.

    Returns the close price as float, or None if not found.
    """
    ticker_path = safe_ticker_path(underlying)
    year = trade_date.year
    ohlcv_file = ohlcv_dir / ticker_path / f"{year}.parquet"

    if not ohlcv_file.exists():
        logger.warning(f"OHLCV file not found: {ohlcv_file}")
        return None

    try:
        ohlcv_df = pd.read_parquet(ohlcv_file)
    except Exception as exc:
        logger.warning(f"Failed to read OHLCV file {ohlcv_file}: {exc}")
        return None

    if 'close' not in ohlcv_df.columns:
        logger.warning(f"'close' column missing in {ohlcv_file}")
        return None

    # Normalise the date column — it may be date, datetime, or string
    date_col = None
    for candidate in ('date', 'Date', 'DATE'):
        if candidate in ohlcv_df.columns:
            date_col = candidate
            break

    if date_col is None:
        # Try the index
        ohlcv_df = ohlcv_df.reset_index()
        if 'index' in ohlcv_df.columns:
            ohlcv_df = ohlcv_df.rename(columns={'index': 'date'})
            date_col = 'date'
        else:
            logger.warning(f"No date column found in {ohlcv_file}")
            return None

    # Coerce to date for comparison
    ohlcv_df['_date_norm'] = pd.to_datetime(ohlcv_df[date_col], errors='coerce').dt.date
    row = ohlcv_df[ohlcv_df['_date_norm'] == trade_date]

    if row.empty:
        logger.warning(f"No OHLCV row for {underlying} on {trade_date} in {ohlcv_file}")
        return None

    close_val = row['close'].iloc[0]
    try:
        close_float = float(close_val)
    except (TypeError, ValueError):
        logger.warning(f"Non-numeric close value for {underlying} on {trade_date}: {close_val!r}")
        return None

    if np.isnan(close_float) or close_float <= 0:
        logger.warning(f"Invalid close price ({close_float}) for {underlying} on {trade_date}")
        return None

    return close_float


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_file(
    input_path: Path,
    output_path: Path,
    ohlcv_dir: Path | None,
    risk_free_rate: float,
) -> tuple[int, int]:
    """
    Process one date's options Parquet file and write output with greek columns.

    Returns (total_rows, greeks_computed) where greeks_computed is the count
    of rows where at least one greek is non-NaN.
    """
    # Derive underlying and trade_date from the path: .../underlying/YYYY-MM-DD.parquet
    underlying = input_path.parent.name
    date_str = input_path.stem  # e.g. "2023-01-03"
    try:
        trade_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Cannot parse date from filename: {input_path.name}")
        raise

    # Load the options data
    df = pd.read_parquet(input_path)
    total_rows = len(df)

    # Determine spot price
    spot_price: float | None = None
    if ohlcv_dir is not None:
        spot_price = get_spot_price(underlying, trade_date, ohlcv_dir)
        if spot_price is None:
            logger.warning(
                f"{underlying} {trade_date}: spot price not found — greeks will all be NaN."
            )

    # Compute days_to_expiry → t (time in years)
    # Expect columns: 'expiration' (date/datetime), 'implied_volatility', 'option_type', 'strike'
    def _to_date(val) -> date | None:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        try:
            return datetime.strptime(str(val)[:10], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None

    def _compute_greeks_for_row(row) -> dict:
        # Option type flag
        opt_type = str(row.get('option_type', '') or '').lower()
        if 'call' in opt_type:
            flag = 'c'
        elif 'put' in opt_type:
            flag = 'p'
        else:
            return dict(_NAN_GREEKS)

        # Expiration → time to expiry in years
        expiration = _to_date(row.get('expiration'))
        if expiration is None:
            return dict(_NAN_GREEKS)
        days_to_expiry = (expiration - trade_date).days
        t = days_to_expiry / 365.0

        # Spot, strike, IV
        S = spot_price  # may be None
        if S is None:
            S = float('nan')

        try:
            K = float(row.get('strike', float('nan')))
        except (TypeError, ValueError):
            K = float('nan')

        try:
            sigma = float(row.get('implied_volatility', float('nan')))
        except (TypeError, ValueError):
            sigma = float('nan')

        return compute_greeks_row(flag, S, K, t, risk_free_rate, sigma)

    # Apply greeks row-by-row via DataFrame.apply
    greeks_list = df.apply(_compute_greeks_for_row, axis=1).tolist()
    greeks_df = pd.DataFrame(greeks_list, index=df.index)

    # Attach greek columns as float64
    for col in ('delta', 'gamma', 'theta', 'vega', 'rho'):
        df[col] = greeks_df[col].astype('float64')

    # Count rows where at least one greek is non-NaN
    greeks_computed = int(df[['delta', 'gamma', 'theta', 'vega', 'rho']].notna().any(axis=1).sum())

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return total_rows, greeks_computed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            'Add Black-Scholes greeks (delta, gamma, theta, vega, rho) to '
            'options chain Parquet files produced by EP 3.'
        )
    )
    parser.add_argument(
        '--input-dir',
        required=True,
        metavar='DIR',
        help='Root directory of raw options Parquet files (DIR/{underlying}/{date}.parquet)',
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        metavar='DIR',
        help='Root output directory for enriched Parquet files (same layout as --input-dir)',
    )
    parser.add_argument(
        '--ohlcv-dir',
        default=None,
        metavar='DIR',
        help=(
            'Root directory of OHLCV Parquet files used for spot prices '
            '(DIR/{ticker}/{year}.parquet). If omitted, greeks requiring spot '
            'price (all of them) will be NaN.'
        ),
    )
    parser.add_argument(
        '--risk-free-rate',
        type=float,
        default=0.05,
        metavar='RATE',
        help='Annual risk-free rate as a decimal (default: 0.05)',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        default=False,
        help='Overwrite existing output files',
    )

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    ohlcv_dir = Path(args.ohlcv_dir) if args.ohlcv_dir else None

    if not input_dir.exists():
        parser.error(f"--input-dir does not exist: {input_dir}")

    if not PY_VOLLIB_AVAILABLE:
        logger.warning(
            "py_vollib is not available — all greeks will be NaN. "
            "Install with: pip install py_vollib"
        )

    # Discover all input files: input_dir/{underlying}/{date}.parquet
    input_files = sorted(input_dir.glob('*/*.parquet'))

    if not input_files:
        logger.warning(f"No parquet files found under {input_dir}")
        return

    logger.info(f"Found {len(input_files)} file(s) to process under {input_dir}")

    # Summary counters
    files_processed = 0
    files_skipped = 0
    files_failed = 0
    total_rows_all = 0
    failed_files: list[str] = []

    for input_path in input_files:
        underlying = input_path.parent.name
        date_stem = input_path.stem

        # Derive output path (mirror directory layout)
        rel = input_path.relative_to(input_dir)
        out_path = output_dir / rel

        # Idempotency check
        if out_path.exists() and not args.force:
            logger.info(f"Skipping existing output: {out_path}")
            files_skipped += 1
            continue

        try:
            total_rows, greeks_computed = process_file(
                input_path=input_path,
                output_path=out_path,
                ohlcv_dir=ohlcv_dir,
                risk_free_rate=args.risk_free_rate,
            )
            print(
                f"{underlying} {date_stem}: {total_rows} rows processed, "
                f"{greeks_computed} greeks computed (non-NaN)"
            )
            files_processed += 1
            total_rows_all += total_rows
        except Exception as exc:
            logger.error(f"Failed to process {input_path}: {exc}")
            files_failed += 1
            failed_files.append(str(input_path))

    # Summary
    print()
    print("=" * 60)
    print(f"Summary:")
    print(f"  Files processed : {files_processed}")
    print(f"  Files skipped   : {files_skipped}")
    print(f"  Files failed    : {files_failed}")
    print(f"  Total rows      : {total_rows_all}")
    if failed_files:
        print(f"  Failed files:")
        for f in failed_files:
            print(f"    {f}")
    print("=" * 60)


if __name__ == '__main__':
    main()
