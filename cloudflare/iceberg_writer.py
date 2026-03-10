"""
Iceberg writer: upload local Parquet files to R2 and register as Apache Iceberg tables (EP 7).

Usage:
    python iceberg_writer.py --table ohlcv --source-dir data/ohlcv/
    python iceberg_writer.py --table options --source-dir data/options/
    python iceberg_writer.py --table options_greeks --source-dir data/options_with_greeks/ --date 2024-01-15

Environment variables required (set in .env):
    CF_ACCOUNT_ID, CF_API_TOKEN, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME (default: market-data-lakehouse),
    R2_CATALOG_NAME (default: market-data-catalog),
    R2_CATALOG_ENDPOINT
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


def _load_config() -> dict:
    """Load config from environment, reporting all missing required vars at once."""
    required_vars = [
        "CF_ACCOUNT_ID",
        "CF_API_TOKEN",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_CATALOG_ENDPOINT",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print("ERROR: Missing required environment variables:")
        for v in missing:
            print(f"  - {v}")
        print("Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    return {
        "account_id": os.environ["CF_ACCOUNT_ID"],
        "api_token": os.environ["CF_API_TOKEN"],
        "access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "bucket_name": os.environ.get("R2_BUCKET_NAME", "market-data-lakehouse"),
        "catalog_name": os.environ.get("R2_CATALOG_NAME", "market-data-catalog"),
        "catalog_endpoint": os.environ["R2_CATALOG_ENDPOINT"],
    }


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

OHLCV_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=True),
    NestedField(2, "ticker", StringType(), required=True),
    NestedField(3, "open", DoubleType()),
    NestedField(4, "high", DoubleType()),
    NestedField(5, "low", DoubleType()),
    NestedField(6, "close", DoubleType()),
    NestedField(7, "volume", LongType()),
    NestedField(8, "adj_close", DoubleType()),
    NestedField(9, "sma_20", DoubleType()),
    NestedField(10, "sma_50", DoubleType()),
    NestedField(11, "sma_200", DoubleType()),
    NestedField(12, "ema_12", DoubleType()),
    NestedField(13, "ema_26", DoubleType()),
    NestedField(14, "rsi_14", DoubleType()),
    NestedField(15, "macd", DoubleType()),
    NestedField(16, "macd_signal", DoubleType()),
    NestedField(17, "macd_hist", DoubleType()),
    NestedField(18, "bb_upper", DoubleType()),
    NestedField(19, "bb_middle", DoubleType()),
    NestedField(20, "bb_lower", DoubleType()),
    NestedField(21, "bb_bandwidth", DoubleType()),
    NestedField(22, "bb_pct_b", DoubleType()),
    NestedField(23, "atr_14", DoubleType()),
    NestedField(24, "obv", DoubleType()),
)

OPTIONS_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=True),
    NestedField(2, "underlying", StringType(), required=True),
    NestedField(3, "symbol", StringType()),
    NestedField(4, "expiration", DateType()),
    NestedField(5, "strike", DoubleType()),
    NestedField(6, "option_type", StringType()),
    NestedField(7, "bid", DoubleType()),
    NestedField(8, "ask", DoubleType()),
    NestedField(9, "mid_price", DoubleType()),
    NestedField(10, "implied_volatility", DoubleType()),
    NestedField(11, "volume", LongType()),
    NestedField(12, "open_interest", LongType()),
    NestedField(13, "in_the_money", BooleanType()),
)

OPTIONS_GREEKS_SCHEMA = Schema(
    NestedField(1, "date", DateType(), required=True),
    NestedField(2, "underlying", StringType(), required=True),
    NestedField(3, "symbol", StringType()),
    NestedField(4, "expiration", DateType()),
    NestedField(5, "strike", DoubleType()),
    NestedField(6, "option_type", StringType()),
    NestedField(7, "bid", DoubleType()),
    NestedField(8, "ask", DoubleType()),
    NestedField(9, "mid_price", DoubleType()),
    NestedField(10, "implied_volatility", DoubleType()),
    NestedField(11, "volume", LongType()),
    NestedField(12, "open_interest", LongType()),
    NestedField(13, "in_the_money", BooleanType()),
    NestedField(14, "delta", DoubleType()),
    NestedField(15, "gamma", DoubleType()),
    NestedField(16, "theta", DoubleType()),
    NestedField(17, "vega", DoubleType()),
    NestedField(18, "rho", DoubleType()),
)

# ---------------------------------------------------------------------------
# Partition specs
# ---------------------------------------------------------------------------

OHLCV_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="date_day"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="ticker"),
)

OPTIONS_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="date_day"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="underlying"),
)

OPTIONS_GREEKS_PARTITION = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="date_day"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="underlying"),
)

# Table name -> (schema, partition_spec)
TABLE_CONFIGS: dict[str, tuple[Schema, PartitionSpec]] = {
    "ohlcv": (OHLCV_SCHEMA, OHLCV_PARTITION),
    "options": (OPTIONS_SCHEMA, OPTIONS_PARTITION),
    "options_greeks": (OPTIONS_GREEKS_SCHEMA, OPTIONS_GREEKS_PARTITION),
}

# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def _load_iceberg_catalog(cfg: dict):
    """Return a PyIceberg catalog connected to the R2 Data Catalog REST endpoint."""
    return load_catalog(
        "r2_catalog",
        **{
            "type": "rest",
            "uri": cfg["catalog_endpoint"],
            "token": cfg["api_token"],
            "warehouse": f"s3://{cfg['bucket_name']}",
            "s3.endpoint": f"https://{cfg['account_id']}.r2.cloudflarestorage.com",
            "s3.access-key-id": cfg["access_key_id"],
            "s3.secret-access-key": cfg["secret_access_key"],
            "s3.region": "auto",
        },
    )


def _get_or_create_table(catalog, table_name: str, schema: Schema, partition_spec: PartitionSpec):
    """Load the Iceberg table if it exists, otherwise create it."""
    full_name = f"default.{table_name}"
    try:
        table = catalog.load_table(full_name)
        print(f"  Loaded existing table: {full_name}")
        return table
    except NoSuchTableError:
        print(f"  Table not found — creating: {full_name}")
        # Ensure the default namespace exists
        try:
            catalog.create_namespace("default")
        except Exception:
            pass  # namespace likely already exists
        table = catalog.create_table(full_name, schema=schema, partition_spec=partition_spec)
        print(f"  Created table: {full_name}")
        return table


# ---------------------------------------------------------------------------
# Date-existence check
# ---------------------------------------------------------------------------


def _dates_in_table(iceberg_table) -> set[str]:
    """
    Return a set of ISO date strings (YYYY-MM-DD) already present in the table.

    Uses PyIceberg's scan API to read the 'date' column. Falls back to an empty
    set on any error (e.g. empty table, schema mismatch) so the caller can decide
    whether to overwrite or skip.
    """
    try:
        scan = iceberg_table.scan(selected_fields=("date",))
        arrow_result = scan.to_arrow()
        if arrow_result.num_rows == 0:
            return set()
        date_col = arrow_result.column("date")
        # PyArrow date32 columns cast to Python date objects via .to_pylist()
        return {str(d) for d in date_col.to_pylist() if d is not None}
    except Exception as e:
        # Only truly empty table / table-not-found scenarios should return empty set.
        # All other errors (network, auth) emit a warning so operator knows
        # deduplication was skipped.
        error_str = str(e).lower()
        if "nosuchtable" in error_str or "not found" in error_str or "does not exist" in error_str:
            return set()
        print(f"  [warn] Could not read existing dates — deduplication skipped: {e}")
        return set()


# ---------------------------------------------------------------------------
# Source file discovery
# ---------------------------------------------------------------------------


def _discover_parquet_files(source_dir: Path, date_filter: str | None) -> list[Path]:
    """
    Return sorted list of Parquet files under source_dir.

    If date_filter is provided (YYYY-MM-DD), only files whose stem matches the
    date string are included (e.g. ``2024-01-15.parquet``).
    """
    all_files = sorted(source_dir.glob("**/*.parquet"))
    if date_filter:
        all_files = [f for f in all_files if date_filter in f.stem]
    return all_files


# ---------------------------------------------------------------------------
# Write logic
# ---------------------------------------------------------------------------


def _write_file(
    parquet_path: Path,
    iceberg_table,
    existing_dates: set[str],
) -> tuple[int, bool]:
    """
    Read a single Parquet file and append its rows to the Iceberg table.

    Returns (rows_written, was_skipped).
    Skips the file if all dates in it are already present in the table.
    """
    df = pd.read_parquet(parquet_path)

    if df.empty:
        print(f"  [skip] {parquet_path.name} — empty file")
        return 0, True

    # Determine dates present in this file
    if "date" in df.columns:
        parsed_dates = pd.to_datetime(df["date"]).dt.date
        file_dates = {str(d) for d in parsed_dates}
        already_loaded = file_dates & existing_dates
        if already_loaded and already_loaded == file_dates:
            print(f"  [skip] {parquet_path.name} — dates already in table: {sorted(already_loaded)}")
            return 0, True
        if already_loaded:
            print(
                f"  [warn] {parquet_path.name} — partial overlap: "
                f"{sorted(already_loaded)} already loaded; appending remainder"
            )
            df = df[~parsed_dates.astype(str).isin(already_loaded)]

    row_count = len(df)
    print(f"  [write] {parquet_path.name} — {row_count:,} rows")

    # Cast integer columns to proper nullable int before Arrow conversion
    INT_COLS = ("volume", "open_interest", "obv")
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    arrow_schema = iceberg_table.schema().as_arrow()
    arrow_table = pa.Table.from_pandas(df, schema=arrow_schema, safe=True)  # safe=True after explicit cast
    iceberg_table.append(arrow_table)

    return row_count, False


# ---------------------------------------------------------------------------
# Main command
# ---------------------------------------------------------------------------


def cmd_write(cfg: dict, table_name: str, source_dir: Path, date_filter: str | None) -> int:
    """
    Core write command: discover Parquet files, load/create Iceberg table, append rows.
    Returns 0 on success, 1 on error.
    """
    if table_name not in TABLE_CONFIGS:
        print(f"ERROR: Unknown table '{table_name}'. Choose from: {', '.join(TABLE_CONFIGS)}")
        return 1

    if not source_dir.exists():
        print(f"ERROR: Source directory not found: {source_dir}")
        return 1

    schema, partition_spec = TABLE_CONFIGS[table_name]

    parquet_files = _discover_parquet_files(source_dir, date_filter)
    if not parquet_files:
        qualifier = f" matching date '{date_filter}'" if date_filter else ""
        print(f"No Parquet files found in {source_dir}{qualifier}.")
        return 0

    print(f"Found {len(parquet_files)} Parquet file(s) in {source_dir}\n")

    # Connect to catalog and get/create table
    print("Connecting to R2 Data Catalog…")
    catalog = _load_iceberg_catalog(cfg)
    iceberg_table = _get_or_create_table(catalog, table_name, schema, partition_spec)
    print()

    # Load existing dates once to avoid redundant scans
    print("Checking existing data in table…")
    existing_dates = _dates_in_table(iceberg_table)
    if existing_dates:
        print(f"  {len(existing_dates)} date(s) already present in table.")
    else:
        print("  Table is empty — all files will be written.")
    print()

    # Process each file
    total_rows = 0
    files_written = 0
    files_skipped = 0

    for parquet_path in parquet_files:
        rows, skipped = _write_file(parquet_path, iceberg_table, existing_dates)
        if skipped:
            files_skipped += 1
        else:
            total_rows += rows
            files_written += 1

    # Summary
    print()
    print("=" * 50)
    print(f"Summary for table '{table_name}':")
    print(f"  Files written : {files_written}")
    print(f"  Rows written  : {total_rows:,}")
    print(f"  Files skipped : {files_skipped}")
    print("=" * 50)

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Write local Parquet files to R2 Iceberg tables — market-data-lakehouse EP 7",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python iceberg_writer.py --table ohlcv --source-dir data/ohlcv/\n"
            "  python iceberg_writer.py --table options --source-dir data/options/\n"
            "  python iceberg_writer.py --table options_greeks --source-dir data/options_with_greeks/ --date 2024-01-15"
        ),
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=list(TABLE_CONFIGS.keys()),
        help="Target Iceberg table name",
    )
    parser.add_argument(
        "--source-dir",
        required=True,
        type=Path,
        help="Directory containing Parquet files to upload",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only upload the Parquet file for this specific date (optional)",
    )

    args = parser.parse_args()
    cfg = _load_config()

    sys.exit(cmd_write(cfg, args.table, args.source_dir, args.date))


if __name__ == "__main__":
    main()
