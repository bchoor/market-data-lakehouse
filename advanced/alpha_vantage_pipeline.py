"""
Alpha Vantage options pipeline — EP 13 upgrade path ($50/mo).

This script is a drop-in replacement for:
  ingestion/alpaca_options_backfill.py  (EP 3)  +
  ingestion/greeks_calculator.py        (EP 4)

When you're ready to upgrade from Alpaca's free tier to Alpha Vantage:
  - Get an API key at alphavantage.co ($50/mo Premium plan for options history)
  - Add ALPHA_VANTAGE_API_KEY to your .env
  - Run this script instead of the Alpaca + greeks scripts
  - The output schema matches options_with_greeks/ exactly — iceberg_writer.py works unchanged

Alpha Vantage Historical Options endpoint:
  https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol={symbol}&date={YYYY-MM-DD}

Returns: contractID, expiration, strike, type, last, bid, ask, volume, openInterest,
         impliedVolatility, delta, gamma, theta, vega (no rho — set to NaN)
"""
import argparse
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

# Load .env if present
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


class AVRateLimitError(Exception):
    """Raised when Alpha Vantage returns a rate-limit response (HTTP 429 or Note in body)."""

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.alphavantage.co/query"

_SCHEMA_DTYPES: dict[str, str] = {
    'date': 'object',
    'underlying': 'string',
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
    'delta': 'float64',
    'gamma': 'float64',
    'theta': 'float64',
    'vega': 'float64',
    'rho': 'float64',
}

_COLUMN_ORDER = list(_SCHEMA_DTYPES.keys())


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------

def fetch_options_for_date(underlying: str, date_str: str, api_key: str) -> list[dict]:
    """Fetch options data from Alpha Vantage for one underlying/date.

    Parameters
    ----------
    underlying : Underlying ticker symbol (e.g. "SPY")
    date_str   : Date string in YYYY-MM-DD format
    api_key    : Alpha Vantage API key

    Returns a list of raw record dicts from the API response.
    On error, logs the issue and returns an empty list.
    """
    params = {
        "function": "HISTORICAL_OPTIONS",
        "symbol": underlying,
        "date": date_str,
        "apikey": api_key,
    }

    try:
        resp = requests.get(_BASE_URL, params=params, timeout=30)
    except requests.RequestException as exc:
        logger.error(f"{underlying} {date_str}: network error — {exc}")
        return []

    # Auth errors
    if resp.status_code in (401, 403):
        logger.error(
            f"Invalid Alpha Vantage API key (HTTP {resp.status_code}). "
            f"Get or upgrade your key at https://www.alphavantage.co/support/#api-key"
        )
        return []

    # Rate limit (HTTP 429)
    if resp.status_code == 429:
        logger.warning(f"{underlying} {date_str}: HTTP 429 — rate limited")
        raise AVRateLimitError(f"HTTP 429 rate limit for {underlying} {date_str}")

    if not resp.ok:
        logger.warning(f"{underlying} {date_str}: unexpected HTTP {resp.status_code}, skipping")
        return []

    try:
        payload = resp.json()
    except ValueError as exc:
        logger.warning(f"{underlying} {date_str}: failed to parse JSON — {exc}")
        return []

    # Detect Alpha Vantage rate-limit note in body
    if "Note" in payload:
        note = payload["Note"]
        if "API call frequency" in note or "rate limit" in note.lower():
            logger.warning(f"{underlying} {date_str}: AV rate limit note — {note}")
            raise AVRateLimitError(f"AV rate limit note for {underlying} {date_str}: {note}")

    # Detect info / error messages
    if "Information" in payload:
        logger.warning(f"{underlying} {date_str}: AV info message — {payload['Information']}")
        return []
    if "Error Message" in payload:
        logger.warning(f"{underlying} {date_str}: AV error — {payload['Error Message']}")
        return []

    records = payload.get("data", [])
    if not records:
        logger.info(f"no data for {underlying} {date_str}")
        return []

    return records


# ---------------------------------------------------------------------------
# Record parsing
# ---------------------------------------------------------------------------

def _safe_float(value, fallback: float = np.nan) -> float:
    """Convert a value to float, returning fallback on failure."""
    if value is None or value == "" or value == "None":
        return fallback
    try:
        result = float(value)
        return result if np.isfinite(result) else fallback
    except (TypeError, ValueError):
        return fallback


def _safe_int(value, fallback=pd.NA):
    """Convert a value to int, returning fallback on failure."""
    if value is None or value == "" or value == "None":
        return fallback
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def parse_contract(record: dict, underlying: str, date_str: str) -> dict | None:
    """Parse one Alpha Vantage contract record into our output schema dict.

    Maps AV field names to our schema:
      contractID → (used for identification only, not stored)
      type       → option_type ("call" / "put", normalized to lowercase)
      rho        → np.nan (AV doesn't provide rho)
      inTheMoney → in_the_money (use if present; else None)

    Returns None if the record is fatally malformed (missing type/strike).
    """
    # Option type — normalize to "call" or "put"
    raw_type = str(record.get("type", record.get("optionType", ""))).lower().strip()
    if "call" in raw_type:
        option_type = "call"
    elif "put" in raw_type:
        option_type = "put"
    else:
        logger.debug(f"Unknown option type {raw_type!r} in record {record.get('contractID', '?')}, skipping")
        return None

    # Strike — required for meaningful data
    strike = _safe_float(record.get("strike"))
    if np.isnan(strike):
        logger.debug(f"Missing strike for {record.get('contractID', '?')}, skipping")
        return None

    # Expiration
    expiration_raw = record.get("expiration", record.get("expirationDate", None))
    expiration = None
    if expiration_raw:
        try:
            expiration = datetime.strptime(str(expiration_raw)[:10], "%Y-%m-%d").date()
        except ValueError:
            expiration = None

    # Price fields
    bid = _safe_float(record.get("bid"), np.nan)
    ask = _safe_float(record.get("ask"), np.nan)
    mid_price = round((bid + ask) / 2.0, 4) if not (pd.isna(bid) or pd.isna(ask)) else np.nan

    # Greeks — Alpha Vantage provides delta, gamma, theta, vega; no rho
    delta_val = _safe_float(record.get("delta"))
    gamma_val = _safe_float(record.get("gamma"))
    theta_val = _safe_float(record.get("theta"))
    vega_val = _safe_float(record.get("vega"))
    rho_val = np.nan  # AV doesn't provide rho

    # Implied volatility — AV may call it "impliedVolatility" or "iv"
    iv = _safe_float(
        record.get("impliedVolatility", record.get("iv", record.get("implied_volatility")))
    )

    # Volume and open interest
    volume = _safe_int(record.get("volume"))
    open_interest = _safe_int(record.get("openInterest", record.get("open_interest")))

    # in_the_money — use AV field if present, otherwise None (can't determine without spot)
    itm_raw = record.get("inTheMoney", record.get("in_the_money", None))
    if itm_raw is None:
        in_the_money = None
    elif isinstance(itm_raw, bool):
        in_the_money = itm_raw
    else:
        itm_str = str(itm_raw).lower().strip()
        if itm_str in ("true", "yes", "1"):
            in_the_money = True
        elif itm_str in ("false", "no", "0"):
            in_the_money = False
        else:
            in_the_money = None

    return {
        "date": date_str,
        "underlying": underlying,
        "expiration": expiration,
        "strike": strike,
        "option_type": option_type,
        "bid": bid,
        "ask": ask,
        "mid_price": mid_price,
        "implied_volatility": iv,
        "volume": volume,
        "open_interest": open_interest,
        "in_the_money": in_the_money,
        "delta": delta_val,
        "gamma": gamma_val,
        "theta": theta_val,
        "vega": vega_val,
        "rho": rho_val,
    }


# ---------------------------------------------------------------------------
# DataFrame builder
# ---------------------------------------------------------------------------

def rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert a list of parsed row dicts to a typed DataFrame matching the output schema."""
    if not rows:
        return pd.DataFrame(columns=_COLUMN_ORDER)

    df = pd.DataFrame(rows)

    # Numeric float columns
    for col in ("strike", "bid", "ask", "mid_price", "implied_volatility",
                "delta", "gamma", "theta", "vega", "rho"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = np.nan

    # Nullable integer columns
    for col in ("volume", "open_interest"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = pd.array([pd.NA] * len(df), dtype="Int64")

    # String columns
    for col in ("underlying", "option_type"):
        if col in df.columns:
            df[col] = df[col].astype("string")
        else:
            df[col] = pd.array([pd.NA] * len(df), dtype="string")

    # Date column — keep as plain string (YYYY-MM-DD, object dtype)
    if "date" in df.columns:
        df["date"] = df["date"].astype("object")

    # Nullable boolean column
    if "in_the_money" in df.columns:
        df["in_the_money"] = df["in_the_money"].astype("boolean")

    # Ensure all columns are present
    for col in _COLUMN_ORDER:
        if col not in df.columns:
            df[col] = np.nan

    return df[_COLUMN_ORDER]


# ---------------------------------------------------------------------------
# Per-date processing
# ---------------------------------------------------------------------------

def process_underlying_date(
    underlying: str,
    date_str: str,
    api_key: str,
    output_dir: Path,
    force: bool,
    sleep_seconds: float,
) -> bool:
    """Fetch, parse, and save one underlying/date.

    Returns True if data was written, False if skipped or failed.
    Sleeps for sleep_seconds after the API call to respect rate limits.
    """
    # Derive output path: output_dir/{underlying}/{date}.parquet
    out_path = output_dir / underlying / f"{date_str}.parquet"

    # Idempotency check
    if out_path.exists() and not force:
        logger.info(f"{underlying} {date_str}: skipping existing file {out_path}")
        return False

    # Fetch raw records
    try:
        records = fetch_options_for_date(underlying, date_str, api_key)
    except Exception as exc:
        logger.error(f"{underlying} {date_str}: fetch error — {exc}")
        return False
    finally:
        # Always sleep to respect rate limits, even on error
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if not records:
        # Already logged inside fetch_options_for_date
        return False

    # Parse records
    parsed_rows = []
    for record in records:
        try:
            row = parse_contract(record, underlying, date_str)
            if row is not None:
                parsed_rows.append(row)
        except Exception as exc:
            logger.debug(f"{underlying} {date_str}: failed to parse record {record.get('contractID', '?')} — {exc}")
            continue

    if not parsed_rows:
        logger.info(f"no data for {underlying} {date_str} (all records failed to parse)")
        return False

    # Build DataFrame and save
    try:
        df = rows_to_dataframe(parsed_rows)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        print(f"{underlying} {date_str}: {len(df)} contracts")
        return True
    except Exception as exc:
        logger.error(f"{underlying} {date_str}: failed to write parquet — {exc}")
        return False


# ---------------------------------------------------------------------------
# Rate-limit retry wrapper
# ---------------------------------------------------------------------------

def fetch_with_retry(
    underlying: str,
    date_str: str,
    api_key: str,
    max_retries: int = 3,
    base_sleep: float = 60.0,
) -> list[dict]:
    """Fetch options data with retry logic for rate-limit responses.

    Calls fetch_options_for_date and retries on AVRateLimitError, sleeping
    base_sleep * (attempt + 1) seconds between attempts.

    Returns the list of raw record dicts, or [] if retries are exhausted.
    """
    for attempt in range(max_retries):
        try:
            return fetch_options_for_date(underlying, date_str, api_key)
        except AVRateLimitError:
            if attempt < max_retries - 1:
                sleep_time = base_sleep * (attempt + 1)
                logger.warning(
                    f"Rate limit hit, sleeping {sleep_time}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(sleep_time)
    logger.error(f"Rate limit retry exhausted for {underlying} {date_str}")
    return []


# ---------------------------------------------------------------------------
# Business day generation
# ---------------------------------------------------------------------------

def generate_business_days(start_str: str, end_str: str) -> list[str]:
    """Return a list of business day date strings (YYYY-MM-DD) between start and end (inclusive).

    Uses pandas bdate_range which skips weekends automatically.
    """
    bdays = pd.bdate_range(start=start_str, end=end_str)
    return [d.strftime("%Y-%m-%d") for d in bdays]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Alpha Vantage historical options pipeline (EP 13). "
            "Drop-in replacement for alpaca_options_backfill.py + greeks_calculator.py. "
            "Requires ALPHA_VANTAGE_API_KEY in environment or .env file."
        )
    )
    parser.add_argument(
        "--underlying",
        nargs="+",
        required=True,
        metavar="SYMBOL",
        help="One or more underlying ticker symbols (e.g. SPY AAPL)",
    )
    parser.add_argument(
        "--start",
        required=True,
        metavar="YYYY-MM-DD",
        help="Start date (inclusive)",
    )
    parser.add_argument(
        "--end",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date (inclusive). Defaults to today.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        metavar="DIR",
        help="Root output directory. Files written to DIR/{underlying}/{date}.parquet",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing output files",
    )
    parser.add_argument(
        "--calls-per-minute",
        type=float,
        default=5.0,
        metavar="N",
        help=(
            "API calls per minute. Default: 5 (free tier). "
            "Set to 75 for Premium ($50/mo) plan."
        ),
    )

    args = parser.parse_args()

    # Validate dates
    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--start must be YYYY-MM-DD, got: {args.start!r}")

    if args.end is not None:
        try:
            end_dt = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            parser.error(f"--end must be YYYY-MM-DD, got: {args.end!r}")
    else:
        end_dt = datetime.today()

    if start_dt > end_dt:
        parser.error(f"--start ({args.start}) must be on or before --end ({end_dt.strftime('%Y-%m-%d')})")

    # API key
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
    if not api_key:
        parser.error(
            "ALPHA_VANTAGE_API_KEY is not set. "
            "Add it to your .env file or export it as an environment variable. "
            "Get a key at https://www.alphavantage.co/support/#api-key"
        )

    # Rate limiting
    calls_per_minute = max(args.calls_per_minute, 0.1)
    sleep_seconds = 60.0 / calls_per_minute
    logger.info(
        f"Rate limiter: {calls_per_minute:.1f} calls/min → {sleep_seconds:.2f}s between requests"
    )

    # Generate business days
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")
    business_days = generate_business_days(start_str, end_str)

    if not business_days:
        logger.warning(f"No business days found between {start_str} and {end_str}")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Summary counters
    total_dates = len(args.underlying) * len(business_days)
    written = 0
    skipped = 0
    failed = 0

    logger.info(
        f"Processing {len(args.underlying)} underlying(s) × {len(business_days)} business days "
        f"= {total_dates} total requests"
    )

    for underlying in args.underlying:
        underlying_written = 0
        underlying_skipped = 0
        underlying_failed = 0

        for date_str in business_days:
            out_path = output_dir / underlying / f"{date_str}.parquet"

            # Idempotency check (fast path — no API call)
            if out_path.exists() and not args.force:
                logger.info(f"{underlying} {date_str}: skipping existing file")
                skipped += 1
                underlying_skipped += 1
                continue

            try:
                success = process_underlying_date(
                    underlying=underlying,
                    date_str=date_str,
                    api_key=api_key,
                    output_dir=output_dir,
                    force=args.force,
                    sleep_seconds=sleep_seconds,
                )
                if success:
                    written += 1
                    underlying_written += 1
                else:
                    # No data / skipped — not a hard failure
                    skipped += 1
                    underlying_skipped += 1
            except Exception as exc:
                logger.error(f"{underlying} {date_str}: unexpected error — {exc}")
                failed += 1
                underlying_failed += 1

        logger.info(
            f"{underlying} complete: {underlying_written} written, "
            f"{underlying_skipped} skipped, {underlying_failed} failed"
        )

    # Final summary
    print()
    print("=" * 60)
    print("Summary:")
    print(f"  Underlyings     : {len(args.underlying)}")
    print(f"  Business days   : {len(business_days)} ({start_str} → {end_str})")
    print(f"  Files written   : {written}")
    print(f"  Files skipped   : {skipped}")
    print(f"  Files failed    : {failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()
