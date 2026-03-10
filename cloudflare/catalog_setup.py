"""
R2 + R2 Data Catalog setup script (EP 6).

Usage:
    python catalog_setup.py --check    # verify infrastructure
    python catalog_setup.py --init     # create R2 bucket + enable Iceberg catalog
    python catalog_setup.py --status   # show detailed status

Environment variables required (set in .env):
    CF_ACCOUNT_ID, CF_API_TOKEN, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME, R2_CATALOG_NAME

NOTE on CF API endpoints:
    The R2 Data Catalog (managed Apache Iceberg metadata) API is accessed at:
        https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/buckets/{bucket}/catalog
    Cloudflare API endpoints evolve — if any request returns 404 or 405, check
    https://developers.cloudflare.com/r2/data-catalog/ for the latest paths.
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

# Load .env from the project root (two levels up from this file: cloudflare/catalog_setup.py)
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


def _load_config() -> dict:
    """Load config from environment, reporting all missing required vars at once."""
    required_vars = [
        "CF_ACCOUNT_ID",
        "CF_API_TOKEN",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_CATALOG_NAME",
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
    }


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

def _s3_client(cfg: dict):
    """Return a boto3 S3 client pointed at R2 (S3-compatible)."""
    return boto3.client(
        "s3",
        endpoint_url=f"https://{cfg['account_id']}.r2.cloudflarestorage.com",
        aws_access_key_id=cfg["access_key_id"],
        aws_secret_access_key=cfg["secret_access_key"],
        region_name="auto",
    )


def _cf_headers(cfg: dict) -> dict:
    """Return standard Cloudflare REST API auth headers."""
    return {
        "Authorization": f"Bearer {cfg['api_token']}",
        "Content-Type": "application/json",
    }


def _catalog_base_url(cfg: dict) -> str:
    """
    Base URL for the R2 Data Catalog API.

    CF Docs: https://developers.cloudflare.com/r2/data-catalog/
    If this endpoint returns 404/405, consult the docs above for updated paths.
    """
    return (
        "https://api.cloudflare.com/client/v4/accounts"
        f"/{cfg['account_id']}/r2/buckets/{cfg['bucket_name']}/catalog"
    )


# ---------------------------------------------------------------------------
# Primitive operations
# ---------------------------------------------------------------------------

def bucket_exists(s3, bucket_name: str) -> bool:
    """Return True if the R2 bucket is accessible (exists + credentials are valid)."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            return False
        # 403 means the bucket exists but we don't own it / wrong credentials
        raise


def catalog_exists(cfg: dict) -> tuple[bool, dict | None]:
    """
    Return (exists, response_json) by querying the CF catalog API.

    A 200 response means the catalog is enabled; 404 means it isn't.
    Other status codes are treated as errors and will raise.
    """
    url = _catalog_base_url(cfg)
    resp = requests.get(url, headers=_cf_headers(cfg), timeout=15)

    if resp.status_code == 200:
        return True, resp.json()
    if resp.status_code == 404:
        return False, None

    # Unexpected — surface the error to the caller
    resp.raise_for_status()
    return False, None  # unreachable but keeps linters happy


def create_bucket(s3, bucket_name: str) -> None:
    """Create the R2 bucket.  R2 does not use LocationConstraint."""
    s3.create_bucket(Bucket=bucket_name)


def enable_catalog(cfg: dict) -> dict:
    """
    Enable (or idempotently re-enable) the R2 Data Catalog for the bucket.

    Uses POST to the catalog endpoint.  If the catalog is already enabled, CF
    may return 200 or 409 — both are treated as success here.

    CF Docs: https://developers.cloudflare.com/r2/data-catalog/
    """
    url = _catalog_base_url(cfg)
    payload = {
        "catalog_name": cfg["catalog_name"],
        "engine": "ICEBERG",
    }
    resp = requests.post(url, json=payload, headers=_cf_headers(cfg), timeout=15)

    # 200/201 = created; 409 = already exists (idempotent)
    if resp.status_code in (200, 201, 409):
        return resp.json() if resp.content else {}

    resp.raise_for_status()
    return {}


# ---------------------------------------------------------------------------
# Bucket stats helper
# ---------------------------------------------------------------------------

def _bucket_stats(s3, bucket_name: str) -> dict:
    """
    Return {object_count, total_size_bytes} by paginating list_objects_v2.
    Large buckets will take a while — this is a one-time setup script so that's OK.
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    count = 0
    total_bytes = 0
    for page in pages:
        for obj in page.get("Contents", []):
            count += 1
            total_bytes += obj.get("Size", 0)

    return {"object_count": count, "total_size_bytes": total_bytes}


def _format_bytes(n: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    value = float(n)
    for unit in units[:-1]:
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} {units[-1]}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_check(cfg: dict) -> int:
    """
    Verify that the R2 bucket and R2 Data Catalog both exist and are accessible.
    Prints ✓ / ✗ for each check.  Returns 0 if all OK, 1 if any fail.
    """
    print("Checking infrastructure…\n")
    s3 = _s3_client(cfg)
    all_ok = True

    # --- Bucket ---
    try:
        ok = bucket_exists(s3, cfg["bucket_name"])
        mark = "✓" if ok else "✗"
        label = "accessible" if ok else "not found or inaccessible"
        print(f"  {mark}  R2 bucket '{cfg['bucket_name']}': {label}")
        if not ok:
            all_ok = False
    except ClientError as exc:
        print(f"  ✗  R2 bucket '{cfg['bucket_name']}': ERROR — {exc}")
        all_ok = False

    # --- Catalog ---
    try:
        ok, _data = catalog_exists(cfg)
        mark = "✓" if ok else "✗"
        label = "enabled" if ok else "not found"
        print(f"  {mark}  R2 Data Catalog '{cfg['catalog_name']}': {label}")
        if not ok:
            all_ok = False
    except (requests.HTTPError, requests.RequestException) as exc:
        print(f"  ✗  R2 Data Catalog '{cfg['catalog_name']}': ERROR — {exc}")
        all_ok = False

    print()
    if all_ok:
        print("All checks passed.")
    else:
        print("One or more checks FAILED.  Run `python catalog_setup.py --init` to create missing resources.")

    return 0 if all_ok else 1


def cmd_init(cfg: dict) -> int:
    """
    Idempotently create the R2 bucket and enable the R2 Data Catalog.
    Prints clear success messages and a .env snippet on completion.
    """
    print("Initialising infrastructure…\n")
    s3 = _s3_client(cfg)

    # --- Bucket ---
    try:
        if bucket_exists(s3, cfg["bucket_name"]):
            print(f"  ✓  R2 bucket '{cfg['bucket_name']}' already exists — skipping create.")
        else:
            print(f"  …  Creating R2 bucket '{cfg['bucket_name']}'…")
            create_bucket(s3, cfg["bucket_name"])
            print(f"  ✓  R2 bucket '{cfg['bucket_name']}' created.")
    except ClientError as exc:
        print(f"  ✗  Failed to create/check R2 bucket: {exc}")
        return 1

    # --- Catalog ---
    try:
        exists, _data = catalog_exists(cfg)
        if exists:
            print(f"  ✓  R2 Data Catalog '{cfg['catalog_name']}' already enabled — skipping.")
        else:
            print(f"  …  Enabling R2 Data Catalog '{cfg['catalog_name']}' (engine=ICEBERG)…")
            enable_catalog(cfg)
            print(f"  ✓  R2 Data Catalog '{cfg['catalog_name']}' enabled.")
    except (requests.HTTPError, requests.RequestException) as exc:
        print(f"  ✗  Failed to enable R2 Data Catalog: {exc}")
        print(
            "     NOTE: CF R2 Data Catalog API endpoints may have changed.\n"
            "     Check https://developers.cloudflare.com/r2/data-catalog/ for current paths."
        )
        return 1

    catalog_endpoint = (
        f"https://api.cloudflare.com/client/v4/accounts"
        f"/{cfg['account_id']}/r2/buckets/{cfg['bucket_name']}/catalog"
    )

    print()
    print("=" * 60)
    print("Setup complete!  Next steps:")
    print("  1. Add the snippet below to your .env file.")
    print("  2. Run ingestion scripts to populate the lakehouse.")
    print("  3. Use the MCP query interface to explore your data.")
    print()
    print("# Add to your .env:")
    print(f"R2_CATALOG_ENDPOINT={catalog_endpoint}")
    print("=" * 60)

    return 0


def cmd_status(cfg: dict) -> int:
    """
    Print detailed status: bucket stats and catalog info.
    """
    print("Fetching status…\n")
    s3 = _s3_client(cfg)
    exit_code = 0

    # --- Bucket ---
    print(f"Bucket: {cfg['bucket_name']}")
    try:
        if not bucket_exists(s3, cfg["bucket_name"]):
            print("  Status : NOT FOUND")
            print("  Run `python catalog_setup.py --init` to create it.")
            exit_code = 1
        else:
            print("  Status : accessible")

            # Head to get creation date (not available via HeadBucket — use list_buckets)
            try:
                all_buckets = s3.list_buckets().get("Buckets", [])
                match = next((b for b in all_buckets if b["Name"] == cfg["bucket_name"]), None)
                if match:
                    print(f"  Created: {match['CreationDate'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
            except (ClientError, StopIteration, KeyError):
                pass  # best-effort

            # Bucket region — R2 reports 'auto'
            print("  Region : auto (Cloudflare R2)")

            print("  Counting objects (this may take a moment for large buckets)…")
            stats = _bucket_stats(s3, cfg["bucket_name"])
            print(f"  Objects: {stats['object_count']:,}")
            print(f"  Size   : {_format_bytes(stats['total_size_bytes'])}")
    except ClientError as exc:
        print(f"  ERROR  : {exc}")
        exit_code = 1

    print()

    # --- Catalog ---
    print(f"Catalog: {cfg['catalog_name']}")
    try:
        ok, data = catalog_exists(cfg)
        if not ok:
            print("  Status : NOT ENABLED")
            print("  Run `python catalog_setup.py --init` to enable it.")
            exit_code = 1
        else:
            print("  Status : enabled")
            if data:
                # Attempt to surface any useful fields CF returns
                result = data.get("result", data)
                if isinstance(result, dict):
                    for key in ("name", "catalog_name", "engine", "state", "created_at", "table_count"):
                        val = result.get(key)
                        if val is not None:
                            print(f"  {key:<12}: {val}")
            print(f"  Endpoint: {_catalog_base_url(cfg)}")
    except (requests.HTTPError, requests.RequestException) as exc:
        print(f"  ERROR  : {exc}")
        print(
            "  NOTE: CF R2 Data Catalog API endpoints may have changed.\n"
            "  Check https://developers.cloudflare.com/r2/data-catalog/ for current paths."
        )
        exit_code = 1

    return exit_code


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="R2 bucket + R2 Data Catalog (Iceberg) setup — market-data-lakehouse EP 6",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python catalog_setup.py --check    # verify bucket + catalog exist\n"
            "  python catalog_setup.py --init     # create bucket + enable catalog (idempotent)\n"
            "  python catalog_setup.py --status   # show detailed status info"
        ),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="verify that bucket and catalog exist")
    group.add_argument("--init", action="store_true", help="create R2 bucket + enable Iceberg catalog")
    group.add_argument("--status", action="store_true", help="show detailed status information")

    args = parser.parse_args()
    cfg = _load_config()

    if args.check:
        sys.exit(cmd_check(cfg))
    elif args.init:
        sys.exit(cmd_init(cfg))
    elif args.status:
        sys.exit(cmd_status(cfg))


if __name__ == "__main__":
    main()
