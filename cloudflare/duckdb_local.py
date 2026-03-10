"""
Local DuckDB query layer for the market-data-lakehouse (EP 9).

DuckDB reads Iceberg metadata directly from R2 via httpfs — no R2 SQL API
calls needed. This is the free/offline dev layer for local exploration.

Usage:
    python duckdb_local.py --sql "SELECT ticker, close FROM ohlcv WHERE date = '2024-01-15' LIMIT 10"
    python duckdb_local.py --list-tables
    python duckdb_local.py --describe ohlcv

Environment variables required (set in .env):
    CF_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
    R2_BUCKET_NAME  (optional, defaults to "market-data-lakehouse")
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment loading
# ---------------------------------------------------------------------------

# Load .env from the project root (two levels up from cloudflare/duckdb_local.py)
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)


# ---------------------------------------------------------------------------
# DuckDB import guard
# ---------------------------------------------------------------------------

try:
    import duckdb
    import pandas as pd
except ImportError as _e:
    print(f"ERROR: Missing dependency — {_e}")
    print("Install with:  pip install duckdb pandas")
    print("Or via bun:    bun add duckdb pandas  (if using a JS project wrapper)")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class DuckDBLocalClient:
    """
    DuckDB connected to R2 Iceberg tables via httpfs.

    Provides a pandas-friendly query interface equivalent to r2sql_client.py
    but backed by local DuckDB — no R2 SQL API costs, works offline once
    the Iceberg metadata is cached.
    """

    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize DuckDB with iceberg + httpfs extensions and R2 config.

        db_path: ":memory:" for in-memory (default) or a file path for a
                 persistent database that survives between sessions.
        """
        self.conn = duckdb.connect(db_path)
        self._setup_extensions()
        self._configure_r2()
        self._register_views()

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _setup_extensions(self):
        """Install and load iceberg + httpfs extensions."""
        try:
            self.conn.execute("INSTALL iceberg; LOAD iceberg;")
            self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception as e:
            print(f"ERROR: Failed to load DuckDB extensions — {e}")
            print(
                "Ensure you have DuckDB >= 0.10.0 and internet access for the first install.\n"
                "  pip install 'duckdb>=0.10.0'"
            )
            raise

    def _configure_r2(self):
        """Configure DuckDB to access R2 via httpfs (S3-compatible, path-style)."""
        missing = [
            v for v in ("CF_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
            if not os.environ.get(v)
        ]
        if missing:
            print("ERROR: Missing required environment variables:")
            for v in missing:
                print(f"  - {v}")
            print("Copy .env.example to .env and fill in your R2 credentials.")
            raise EnvironmentError(f"Missing env vars: {missing}")

        account_id = os.environ["CF_ACCOUNT_ID"]
        access_key = os.environ["R2_ACCESS_KEY_ID"]
        secret_key = os.environ["R2_SECRET_ACCESS_KEY"]
        bucket = os.environ.get("R2_BUCKET_NAME", "market-data-lakehouse")

        try:
            self.conn.execute(f"SET s3_endpoint='{account_id}.r2.cloudflarestorage.com';")
            self.conn.execute(f"SET s3_access_key_id='{access_key}';")
            self.conn.execute(f"SET s3_secret_access_key='{secret_key}';")
            self.conn.execute("SET s3_url_style='path';")
            self.conn.execute("SET s3_use_ssl=true;")
        except Exception as e:
            print(f"ERROR: Failed to configure R2 S3 settings — {e}")
            print(
                "Check your R2 credentials:\n"
                "  1. CF_ACCOUNT_ID  — found at dash.cloudflare.com (right sidebar)\n"
                "  2. R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY — R2 > Manage API Tokens\n"
                "  3. Ensure the token has 'Object Read' permission on the bucket"
            )
            raise

        self._bucket = bucket

    def _register_views(self):
        """Register Iceberg tables as DuckDB views for easy querying.

        Tables that don't exist in R2 yet are silently skipped — the view
        will be created once data is written.
        """
        tables = {
            "ohlcv": f"s3://{self._bucket}/ohlcv/",
            "options": f"s3://{self._bucket}/options/",
            "options_greeks": f"s3://{self._bucket}/options_with_greeks/",
        }
        for name, path in tables.items():
            try:
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM iceberg_scan('{path}');"
                )
            except Exception:
                # Table may not exist yet — that's fine, skip silently
                pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute arbitrary SQL and return results as a DataFrame."""
        try:
            return self.conn.execute(sql).df()
        except Exception as e:
            _maybe_r2_hint(e)
            raise

    def list_tables(self) -> list[str]:
        """Return names of all registered views and tables."""
        df = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name;"
        ).df()
        return df["table_name"].tolist()

    def describe_table(self, table: str) -> pd.DataFrame:
        """Return schema information for *table* as a DataFrame."""
        try:
            return self.conn.execute(f"DESCRIBE {table};").df()
        except Exception as e:
            _maybe_r2_hint(e)
            raise

    def sample(self, table: str, n: int = 5) -> pd.DataFrame:
        """Return the first *n* rows of *table*."""
        return self.query(f"SELECT * FROM {table} LIMIT {n};")

    def scan_iceberg(self, table_path: str):
        """Low-level iceberg_scan for a specific S3 path.

        Returns a raw DuckDB result object (call .df() to get a DataFrame).

        Example:
            result = client.scan_iceberg("s3://my-bucket/custom_table/")
            df = result.df()
        """
        return self.conn.execute(f"SELECT * FROM iceberg_scan('{table_path}')")

    def close(self):
        """Close the underlying DuckDB connection."""
        self.conn.close()

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------

def _maybe_r2_hint(exc: Exception) -> None:
    """If *exc* looks like an S3/R2 connectivity error, print a helpful hint."""
    msg = str(exc).lower()
    if any(kw in msg for kw in ("s3", "http", "ssl", "connection", "credential", "403", "401")):
        print(
            "\nHint: R2 connection error detected.  Check the following:\n"
            "  1. CF_ACCOUNT_ID is correct (no leading/trailing spaces)\n"
            "  2. R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY are valid\n"
            "  3. The R2 token has 'Object Read' permission on the bucket\n"
            "  4. R2_BUCKET_NAME matches the actual bucket name\n"
            "  5. Network is reachable (DuckDB needs internet for httpfs)\n"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Local DuckDB query layer against R2 Iceberg tables — EP 9\n\n"
            "DuckDB reads Iceberg metadata directly from R2 via httpfs.\n"
            "No R2 SQL API calls — the free/offline dev alternative."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python duckdb_local.py --sql \"SELECT ticker, close FROM ohlcv WHERE date = '2024-01-15' LIMIT 10\"\n"
            "  python duckdb_local.py --list-tables\n"
            "  python duckdb_local.py --describe ohlcv"
        ),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", metavar="QUERY", help="Execute a SQL query and print results")
    group.add_argument("--list-tables", action="store_true", help="List registered views/tables")
    group.add_argument("--describe", metavar="TABLE", help="Show schema for TABLE")

    return parser


def main() -> None:
    try:
        from tabulate import tabulate
    except ImportError:
        print("ERROR: tabulate not installed.  Run: pip install tabulate")
        sys.exit(1)

    parser = _build_parser()
    args = parser.parse_args()

    try:
        client = DuckDBLocalClient()
    except (EnvironmentError, Exception) as e:
        sys.exit(1)

    try:
        if args.sql:
            df = client.query(args.sql)
            if df.empty:
                print("(no rows returned)")
            else:
                print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

        elif args.list_tables:
            tables = client.list_tables()
            if not tables:
                print("No tables/views registered.")
            else:
                print(tabulate({"table": tables}, headers="keys", tablefmt="psql", showindex=False))

        elif args.describe:
            df = client.describe_table(args.describe)
            print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
