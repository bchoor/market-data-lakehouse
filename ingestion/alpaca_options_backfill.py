"""
Alpaca Markets options chain backfill script (EP 3).

NOTE: Alpaca free tier provides current-day options chain snapshots.
Historical EOD chain data requires a paid Alpaca subscription or an
alternative data source (see advanced/alpha_vantage_pipeline.py for
the $50/mo Alpha Vantage upgrade path).

This script fetches available options data for the given tickers and
date range, writing Parquet files for use with the Iceberg writer (EP 7).

Usage:
    python alpaca_options_backfill.py --underlying SPY AAPL --start 2022-01-01 --end 2023-12-31 --output-dir data/options/
    python alpaca_options_backfill.py --underlying SPY --start 2022-01-01 --end 2023-12-31 --output-dir data/options/ --force
"""
import argparse
import os
import sys
import time
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Alpaca SDK imports
# ---------------------------------------------------------------------------
try:
    from alpaca.data.historical.option import OptionHistoricalDataClient
    from alpaca.data.requests import OptionChainRequest
    from alpaca.data.enums import OptionsFeed
    from alpaca.data.requests import OptionBarsRequest
    from alpaca.data.timeframe import TimeFrame
except ImportError as exc:
    logger.error(
        "alpaca-py is not installed. Run: pip install alpaca-py>=0.20.0\n"
        f"Original error: {exc}"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Rate-limit / retry helpers
# ---------------------------------------------------------------------------
_RETRY_BASE_SLEEP = 1.0   # seconds
_RETRY_MAX_SLEEP = 60.0   # seconds
_RETRY_MAX_ATTEMPTS = 5


def _sleep_backoff(attempt: int) -> None:
    """Exponential backoff sleep before a retry attempt (0-indexed)."""
    sleep_for = min(_RETRY_BASE_SLEEP * (2 ** attempt), _RETRY_MAX_SLEEP)
    logger.warning(f"Rate-limited. Sleeping {sleep_for:.1f}s before retry {attempt + 1}/{_RETRY_MAX_ATTEMPTS}.")
    time.sleep(sleep_for)


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if the exception looks like a 429 or 503."""
    msg = str(exc).lower()
    return any(code in msg for code in ('429', '503', 'too many requests', 'rate limit'))


def _is_no_data_error(exc: Exception) -> bool:
    """Return True if the exception indicates missing data (404, 422)."""
    msg = str(exc).lower()
    return any(code in msg for code in ('404', '422', 'not found', 'unprocessable'))


# ---------------------------------------------------------------------------
# Alpaca client factory
# ---------------------------------------------------------------------------

def build_client() -> OptionHistoricalDataClient:
    """Build an OptionHistoricalDataClient using env credentials."""
    api_key = os.environ.get('ALPACA_API_KEY')
    secret_key = os.environ.get('ALPACA_SECRET_KEY')
    if not api_key or not secret_key:
        logger.warning(
            "ALPACA_API_KEY or ALPACA_SECRET_KEY not set. "
            "Proceeding without authentication (lower rate limits apply)."
        )
        return OptionHistoricalDataClient()
    return OptionHistoricalDataClient(api_key=api_key, secret_key=secret_key)


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_option_chain_snapshot(
    client: OptionHistoricalDataClient,
    underlying: str,
    snapshot_date: date | None = None,
) -> list[dict]:
    """
    Fetch the options chain snapshot for *underlying* on *snapshot_date*.

    Returns a list of row dicts matching the target schema.
    On free tier this is always the current chain — historical snapshots
    are not available without a paid subscription.
    """
    for attempt in range(_RETRY_MAX_ATTEMPTS):
        try:
            # Build request kwargs; add date if the SDK accepts it
            req_kwargs: dict = dict(
                underlying_symbol=underlying,
                feed=OptionsFeed.INDICATIVE,
            )
            if snapshot_date is not None:
                # Attempt to pass date; SDK may or may not support it
                try:
                    req = OptionChainRequest(**req_kwargs, date=snapshot_date)
                except TypeError:
                    req = OptionChainRequest(**req_kwargs)
            else:
                req = OptionChainRequest(**req_kwargs)
            chain = client.get_option_chain(req)
            # chain is a dict keyed by option symbol → OptionSnapshot
            rows = []
            for symbol, snap in chain.items():
                row = _snapshot_to_row(underlying, symbol, snap)
                if row is not None:
                    rows.append(row)
            return rows
        except Exception as exc:
            if _is_rate_limit_error(exc):
                if attempt < _RETRY_MAX_ATTEMPTS - 1:
                    _sleep_backoff(attempt)
                    continue
                logger.error(f"Max retries reached for {underlying} snapshot.")
                return []
            if _is_no_data_error(exc):
                logger.info(f"No snapshot data for {underlying} on {snapshot_date}: {exc}")
                return []
            # Unexpected error — log and return empty
            logger.error(f"Unexpected error fetching snapshot for {underlying}: {exc}")
            return []
    return []


def _snapshot_to_row(underlying: str, symbol: str, snap) -> dict | None:
    """Convert an Alpaca OptionSnapshot object to a flat row dict.

    Returns None if the contract cannot be classified as call or put (caller
    should skip None entries).
    """
    greeks = getattr(snap, 'greeks', None)
    details = getattr(snap, 'details', None)

    strike = float(getattr(details, 'strike_price', 0.0) or 0.0) if details else 0.0
    expiration_raw = getattr(details, 'expiry_date', None) if details else None
    option_type_raw = getattr(details, 'option_type', None) if details else None

    # Normalise option_type to "call" / "put"
    if option_type_raw is not None:
        ot_str = str(option_type_raw).lower()
        if 'call' in ot_str:
            option_type = 'call'
        elif 'put' in ot_str:
            option_type = 'put'
        else:
            logger.warning(f"Unrecognised option_type: {option_type_raw!r}, skipping contract")
            return None
    else:
        option_type = None

    # Expiration date
    if expiration_raw is not None:
        if isinstance(expiration_raw, (date, datetime)):
            expiration = expiration_raw if isinstance(expiration_raw, date) else expiration_raw.date()
        else:
            try:
                expiration = datetime.strptime(str(expiration_raw)[:10], '%Y-%m-%d').date()
            except ValueError:
                expiration = None
    else:
        expiration = None

    # Quote fields
    quote = getattr(snap, 'latest_quote', None)
    bid = float(getattr(quote, 'bid_price', 0.0) or 0.0) if quote else 0.0
    ask = float(getattr(quote, 'ask_price', 0.0) or 0.0) if quote else 0.0
    mid_price = round((bid + ask) / 2, 4) if (bid or ask) else 0.0

    # Trade / misc fields
    iv = float(getattr(snap, 'implied_volatility', 0.0) or 0.0)
    day = getattr(snap, 'day', None)
    volume = int(getattr(day, 'volume', 0) or 0) if day is not None else 0
    open_interest = int(getattr(snap, 'open_interest', 0) or 0)
    in_the_money = bool(getattr(snap, 'in_the_money', False) or False)

    return {
        'symbol': symbol,
        'underlying': underlying,
        'expiration': expiration,
        'strike': strike,
        'option_type': option_type,
        'bid': bid,
        'ask': ask,
        'mid_price': mid_price,
        'implied_volatility': iv,
        'volume': volume,
        'open_interest': open_interest,
        'in_the_money': in_the_money,
    }


# ---------------------------------------------------------------------------
# Date iteration helpers
# ---------------------------------------------------------------------------

def _date_range(start: date, end: date) -> list[date]:
    """Return all calendar dates from start up to and including end."""
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _is_weekday(d: date) -> bool:
    return d.weekday() < 5  # Mon=0 … Fri=4


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def output_path(output_dir: Path, underlying: str, d: date) -> Path:
    """Return the canonical output path for a given underlying + date."""
    return output_dir / underlying / f"{d.isoformat()}.parquet"


_SCHEMA_DTYPES: dict[str, str] = {
    'date': 'object',           # will be cast to date before saving
    'underlying': 'string',
    'symbol': 'string',
    'expiration': 'object',
    'strike': 'float64',
    'option_type': 'string',
    'bid': 'float64',
    'ask': 'float64',
    'mid_price': 'float64',
    'implied_volatility': 'float64',
    'volume': 'Int64',
    'open_interest': 'Int64',
    'in_the_money': 'boolean',
}

_COLUMN_ORDER = list(_SCHEMA_DTYPES.keys())


def rows_to_dataframe(rows: list[dict], snapshot_date: date, underlying: str) -> pd.DataFrame:
    """Convert a list of row dicts to a typed DataFrame with the target schema."""
    if not rows:
        return pd.DataFrame(columns=_COLUMN_ORDER)

    df = pd.DataFrame(rows)
    df['date'] = snapshot_date
    df['underlying'] = underlying

    # Cast numeric columns safely
    for col in ('strike', 'bid', 'ask', 'mid_price', 'implied_volatility'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        else:
            df[col] = 0.0

    for col in ('volume', 'open_interest'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        else:
            df[col] = pd.array([0] * len(df), dtype='Int64')

    if 'in_the_money' in df.columns:
        df['in_the_money'] = df['in_the_money'].astype('boolean')
    else:
        df['in_the_money'] = pd.array([False] * len(df), dtype='boolean')

    if 'option_type' not in df.columns:
        df['option_type'] = pd.NA
    df['option_type'] = df['option_type'].astype('string')

    # Cast date column to proper date dtype
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Ensure all schema columns are always present with appropriate defaults
    for col, dtype in _SCHEMA_DTYPES.items():
        if col not in df.columns:
            if dtype == 'boolean':
                df[col] = pd.array([False] * len(df), dtype='boolean')
            elif dtype in ('Int64',):
                df[col] = pd.array([0] * len(df), dtype='Int64')
            elif dtype == 'float64':
                df[col] = 0.0
            elif dtype == 'string':
                df[col] = pd.array([''] * len(df), dtype='string')
            else:
                df[col] = pd.NA

    return df[_COLUMN_ORDER]


def save_dataframe(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to Parquet, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def process_underlying(
    client: OptionHistoricalDataClient,
    underlying: str,
    start: date,
    end: date,
    output_dir: Path,
    force: bool,
) -> None:
    """
    Iterate over each trading date in [start, end] and write one Parquet
    file per date for *underlying*.

    Free-tier note: Because Alpaca free tier only exposes the *current*
    chain snapshot, each date in the loop will receive the same snapshot
    data (today's chain). A paid subscription is needed for true
    historical EOD snapshots.
    """
    dates = [d for d in _date_range(start, end) if _is_weekday(d)]

    if not dates:
        logger.warning(f"{underlying}: no weekdays in [{start}, {end}] — nothing to do.")
        return

    logger.warning(
        f"[FREE-TIER NOTICE] Alpaca free tier returns the current chain snapshot, not historical EOD data. "
        f"All {len(dates)} date files for {underlying} will contain today's snapshot."
    )

    for d in dates:
        out = output_path(output_dir, underlying, d)

        if out.exists() and not force:
            logger.info(f"{underlying} {d}: skipping existing file {out}")
            continue

        # Fetch per-date (free tier ignores the date, paid tier uses it)
        rows = fetch_option_chain_snapshot(client, underlying, snapshot_date=d)

        if not rows:
            logger.warning(f"{underlying} {d}: snapshot returned 0 contracts, skipping.")
            continue

        try:
            df = rows_to_dataframe(rows, d, underlying)
            save_dataframe(df, out)
            print(f"{underlying} {d}: {len(df)} contracts")
        except Exception as exc:
            logger.error(f"{underlying} {d}: failed to write parquet — {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Download Alpaca options chain snapshots and write one Parquet file '
            'per underlying per date. See module docstring for free-tier limitations.'
        )
    )
    parser.add_argument(
        '--underlying',
        nargs='+',
        required=True,
        metavar='SYMBOL',
        help='One or more underlying ticker symbols (e.g. SPY AAPL)',
    )
    parser.add_argument(
        '--start',
        required=True,
        metavar='YYYY-MM-DD',
        help='Start date (inclusive)',
    )
    parser.add_argument(
        '--end',
        required=True,
        metavar='YYYY-MM-DD',
        help='End date (inclusive)',
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        metavar='DIR',
        help='Root output directory. Files are written to DIR/{underlying}/{date}.parquet',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        default=False,
        help='Overwrite existing Parquet files',
    )

    args = parser.parse_args(argv)

    # Validate date formats
    try:
        start_dt = datetime.strptime(args.start, '%Y-%m-%d').date()
    except ValueError:
        parser.error(f"--start must be in YYYY-MM-DD format, got: {args.start!r}")
    try:
        end_dt = datetime.strptime(args.end, '%Y-%m-%d').date()
    except ValueError:
        parser.error(f"--end must be in YYYY-MM-DD format, got: {args.end!r}")

    if start_dt >= end_dt:
        parser.error(f"--start ({args.start}) must be strictly before --end ({args.end})")

    # Store parsed date objects back on namespace
    args.start_date = start_dt
    args.end_date = end_dt

    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = build_client()

    failed: list[str] = []

    for underlying in args.underlying:
        try:
            process_underlying(
                client=client,
                underlying=underlying,
                start=args.start_date,
                end=args.end_date,
                output_dir=output_dir,
                force=args.force,
            )
            logger.info(f"Completed {underlying}")
        except Exception as exc:
            logger.error(f"Failed to process {underlying}: {exc}")
            failed.append(underlying)

    if failed:
        logger.error(f"Failed underlyings: {failed}")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
