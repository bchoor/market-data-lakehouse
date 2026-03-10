"""
Daily update orchestrator for the market-data-lakehouse pipeline (EP 5).

Calls yfinance_backfill, alpaca_options_backfill, and greeks_calculator in
order, maintaining a checkpoint file so incremental runs only fetch missing
dates.

Usage:
    python incremental_runner.py --tickers SPY AAPL ^VIX --underlyings SPY AAPL --mode backfill
    python incremental_runner.py --tickers SPY AAPL --underlyings SPY --mode incremental
    python incremental_runner.py --tickers SPY --underlyings SPY --mode incremental \
        --ohlcv-dir data/ohlcv --options-dir data/options --greeks-dir data/options_with_greeks
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

# Load .env if present
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_OHLCV_DIR = "data/ohlcv"
DEFAULT_OPTIONS_DIR = "data/options"
DEFAULT_GREEKS_DIR = "data/options_with_greeks"
DEFAULT_CHECKPOINT_FILE = "data/checkpoint.json"
DEFAULT_LOOKBACK_YEARS = 2
MAX_LOOKBACK_YEARS = 3

DATE_FMT = "%Y-%m-%d"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint(path: Path) -> dict:
    """Load checkpoint from JSON file, returning an empty structure if absent."""
    if path.exists():
        try:
            with path.open() as fh:
                data = json.load(fh)
            logger.info("Loaded checkpoint from %s", path)
            return data
        except json.JSONDecodeError as exc:
            logger.warning(
                "Checkpoint file %s is corrupted and could not be parsed (%s) — starting fresh",
                path,
                exc,
            )
        except OSError as exc:
            logger.warning("Could not read checkpoint %s: %s — starting fresh", path, exc)
    return {"last_run": None, "ohlcv": {}, "options": {}, "greeks": {}}


def save_checkpoint(checkpoint: dict, path: Path) -> None:
    """Persist checkpoint to JSON file atomically (write-to-temp then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".checkpoint-")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(checkpoint, fh, indent=2, default=str)
        os.replace(tmp, path)  # atomic on POSIX
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    logger.debug("Checkpoint saved to %s", path)


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def parse_date(value: str, flag: str) -> date:
    """Parse a YYYY-MM-DD string, raising ArgumentTypeError on failure."""
    try:
        return datetime.strptime(value, DATE_FMT).date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{flag} must be in YYYY-MM-DD format, got: {value!r}"
        )


def default_start() -> date:
    return date.today() - timedelta(days=DEFAULT_LOOKBACK_YEARS * 365)


def yesterday() -> date:
    return date.today() - timedelta(days=1)


def next_day(date_str: str) -> str:
    """Return the day after date_str as a YYYY-MM-DD string."""
    d = datetime.strptime(date_str, DATE_FMT).date()
    return (d + timedelta(days=1)).strftime(DATE_FMT)


# ---------------------------------------------------------------------------
# Incremental start-date helpers
# ---------------------------------------------------------------------------

def ohlcv_start_for(ticker: str, checkpoint: dict, fallback: date) -> date:
    last = checkpoint.get("ohlcv", {}).get(ticker)
    if last:
        return datetime.strptime(next_day(last), DATE_FMT).date()
    return fallback


def options_start_for(underlying: str, checkpoint: dict, fallback: date) -> date:
    last = checkpoint.get("options", {}).get(underlying)
    if last:
        return datetime.strptime(next_day(last), DATE_FMT).date()
    return fallback


def greeks_start_for(underlying: str, checkpoint: dict, fallback: date) -> date:
    last = checkpoint.get("greeks", {}).get(underlying)
    if last:
        return datetime.strptime(next_day(last), DATE_FMT).date()
    return fallback


# ---------------------------------------------------------------------------
# Subprocess runners
# ---------------------------------------------------------------------------

def _ingestion_dir() -> Path:
    """Absolute path to the ingestion/ directory (where this script lives)."""
    return Path(__file__).parent.resolve()


def run_ohlcv(tickers: list[str], start: str, end: str, output_dir: str) -> tuple[bool, str]:
    """Run yfinance_backfill.py as a subprocess."""
    script = str(_ingestion_dir() / "yfinance_backfill.py")
    cmd = [
        sys.executable, script,
        "--tickers", *tickers,
        "--start", start,
        "--end", end,
        "--output-dir", output_dir,
    ]
    logger.info("OHLCV: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip()
        return False, msg
    return True, result.stdout.strip()


def run_options(underlyings: list[str], start: str, end: str, output_dir: str) -> tuple[bool, str]:
    """Run alpaca_options_backfill.py as a subprocess."""
    script = str(_ingestion_dir() / "alpaca_options_backfill.py")
    cmd = [
        sys.executable, script,
        "--underlying", *underlyings,
        "--start", start,
        "--end", end,
        "--output-dir", output_dir,
    ]
    logger.info("Options: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip()
        return False, msg
    return True, result.stdout.strip()


def run_greeks(input_dir: str, output_dir: str, ohlcv_dir: str) -> tuple[bool, str]:
    """Run greeks_calculator.py as a subprocess."""
    script = str(_ingestion_dir() / "greeks_calculator.py")
    cmd = [
        sys.executable, script,
        "--input-dir", input_dir,
        "--output-dir", output_dir,
        "--ohlcv-dir", ohlcv_dir,
    ]
    logger.info("Greeks: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip()
        return False, msg
    return True, result.stdout.strip()


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> None:
    checkpoint_path = Path(args.checkpoint)
    checkpoint = load_checkpoint(checkpoint_path)

    fallback_start = args.start or default_start()
    end_date = args.end or yesterday()

    failures: list[str] = []

    # Summary accumulators
    ohlcv_summary: list[str] = []
    options_summary: list[str] = []
    greeks_summary: list[str] = []

    # -----------------------------------------------------------------------
    # Step 1: OHLCV via yfinance_backfill
    # -----------------------------------------------------------------------
    logger.info("=== Step 1: OHLCV ===")

    if args.mode == "incremental":
        # Group tickers by their individual start dates
        groups: dict[str, list[str]] = {}
        for ticker in args.tickers:
            sd = ohlcv_start_for(ticker, checkpoint, fallback_start)
            key = sd.strftime(DATE_FMT)
            groups.setdefault(key, []).append(ticker)
    else:
        # backfill: all tickers share the same start
        groups = {fallback_start.strftime(DATE_FMT): list(args.tickers)}

    end_str = end_date.strftime(DATE_FMT)

    for start_str, group_tickers in groups.items():
        if start_str > end_str:
            logger.info(
                "OHLCV tickers %s already up-to-date (checkpoint >= end date), skipping.",
                group_tickers,
            )
            ohlcv_summary.extend(
                f"{t} (already up-to-date)" for t in group_tickers
            )
            continue

        ok, output = run_ohlcv(group_tickers, start_str, end_str, args.ohlcv_dir)
        if ok:
            for ticker in group_tickers:
                checkpoint.setdefault("ohlcv", {})[ticker] = end_str
                ohlcv_summary.append(f"{ticker} ({start_str} -> {end_str})")
            checkpoint["last_run"] = date.today().strftime(DATE_FMT)
            save_checkpoint(checkpoint, checkpoint_path)
        else:
            for ticker in group_tickers:
                failures.append(f"ohlcv:{ticker}")
                ohlcv_summary.append(f"{ticker} (FAILED)")
            logger.error("OHLCV failed for %s: %s", group_tickers, output)

    # -----------------------------------------------------------------------
    # Step 2: Options chains via alpaca_options_backfill
    # -----------------------------------------------------------------------
    logger.info("=== Step 2: Options ===")

    if args.mode == "incremental":
        opt_groups: dict[str, list[str]] = {}
        for underlying in args.underlyings:
            sd = options_start_for(underlying, checkpoint, fallback_start)
            key = sd.strftime(DATE_FMT)
            opt_groups.setdefault(key, []).append(underlying)
    else:
        opt_groups = {fallback_start.strftime(DATE_FMT): list(args.underlyings)}

    for start_str, group_underlyings in opt_groups.items():
        if start_str > end_str:
            logger.info(
                "Options underlyings %s already up-to-date, skipping.",
                group_underlyings,
            )
            options_summary.extend(
                f"{u} (already up-to-date)" for u in group_underlyings
            )
            continue

        ok, output = run_options(group_underlyings, start_str, end_str, args.options_dir)
        if ok:
            for u in group_underlyings:
                checkpoint.setdefault("options", {})[u] = end_str
                options_summary.append(f"{u} ({start_str} -> {end_str})")
            save_checkpoint(checkpoint, checkpoint_path)
        else:
            for u in group_underlyings:
                failures.append(f"options:{u}")
                options_summary.append(f"{u} (FAILED)")
            logger.error("Options failed for %s: %s", group_underlyings, output)

    # -----------------------------------------------------------------------
    # Step 3: Greeks calculation
    # -----------------------------------------------------------------------
    logger.info("=== Step 3: Greeks ===")

    options_all_failed = len(args.underlyings) > 0 and all(
        f"options:{u}" in failures for u in args.underlyings
    )
    if options_all_failed:
        logger.warning("Skipping greeks step — options failed for all underlyings")
        for u in args.underlyings:
            failures.append(f"greeks:{u} (skipped — options failed)")
            greeks_summary.append(f"{u} (SKIPPED — options step failed for all underlyings)")
    else:
        ok, output = run_greeks(args.options_dir, args.greeks_dir, args.ohlcv_dir)
        if ok:
            # Use the minimum successfully-checkpointed options date as the greeks checkpoint
            options_dates = [
                v for k, v in checkpoint.get("options", {}).items()
                if k in args.underlyings
            ]
            greeks_checkpoint_date = min(options_dates) if options_dates else end_str
            for u in args.underlyings:
                checkpoint.setdefault("greeks", {})[u] = greeks_checkpoint_date
                greeks_summary.append(f"{u} ({greeks_checkpoint_date})")
            save_checkpoint(checkpoint, checkpoint_path)
        else:
            for u in args.underlyings:
                failures.append(f"greeks:{u}")
                greeks_summary.append(f"{u} (FAILED)")
            logger.error("Greeks calculation failed: %s", output)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print("\n=== Run Summary ===")
    print(f"Mode: {args.mode}")
    print(f"OHLCV: {', '.join(ohlcv_summary) if ohlcv_summary else 'none'}")
    print(f"Options: {', '.join(options_summary) if options_summary else 'none'}")
    print(f"Greeks: {', '.join(greeks_summary) if greeks_summary else 'none'}")
    print(f"Failed: {', '.join(failures) if failures else 'none'}")

    tickers_arg = " ".join(args.tickers)
    underlyings_arg = " ".join(args.underlyings)
    print(
        f"\nNext run: python incremental_runner.py "
        f"--tickers {tickers_arg} --underlyings {underlyings_arg} --mode incremental"
    )

    if failures:
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily update orchestrator for the market-data-lakehouse pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="Space-separated list of ticker symbols for OHLCV download (e.g. SPY AAPL ^VIX).",
    )
    parser.add_argument(
        "--underlyings",
        nargs="+",
        required=True,
        help="Space-separated list of underlying symbols for options/greeks (e.g. SPY AAPL).",
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        default="incremental",
        help=(
            "backfill: fetch full date range from --start to yesterday. "
            "incremental: use checkpoint to fetch only missing dates (default: incremental)."
        ),
    )
    parser.add_argument(
        "--start",
        default=None,
        help=(
            f"Override start date (YYYY-MM-DD). "
            f"Defaults to {DEFAULT_LOOKBACK_YEARS} years ago for backfill, "
            "or per-ticker checkpoint + 1 day for incremental."
        ),
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Override end date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--ohlcv-dir",
        default=DEFAULT_OHLCV_DIR,
        help=f"Output directory for OHLCV Parquet files (default: {DEFAULT_OHLCV_DIR}).",
    )
    parser.add_argument(
        "--options-dir",
        default=DEFAULT_OPTIONS_DIR,
        help=f"Output directory for options chain Parquet files (default: {DEFAULT_OPTIONS_DIR}).",
    )
    parser.add_argument(
        "--greeks-dir",
        default=DEFAULT_GREEKS_DIR,
        help=f"Output directory for options+greeks Parquet files (default: {DEFAULT_GREEKS_DIR}).",
    )
    parser.add_argument(
        "--checkpoint",
        default=DEFAULT_CHECKPOINT_FILE,
        help=f"Path to checkpoint JSON file (default: {DEFAULT_CHECKPOINT_FILE}).",
    )

    args = parser.parse_args(argv)

    # Validate --start
    if args.start is not None:
        start_date = parse_date(args.start, "--start")
        three_years_ago = date.today() - timedelta(days=MAX_LOOKBACK_YEARS * 365)
        if start_date < three_years_ago:
            logger.warning(
                "--start %s is more than %d years ago; free-tier data availability "
                "may be limited.",
                args.start,
                MAX_LOOKBACK_YEARS,
            )
        args.start = start_date
    else:
        args.start = default_start() if args.mode == "backfill" else None

    # Validate --end
    if args.end is not None:
        args.end = parse_date(args.end, "--end")
    else:
        args.end = yesterday()

    if args.start is not None and args.start > args.end:
        parser.error(f"--start ({args.start}) must be before --end ({args.end})")

    return args


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
