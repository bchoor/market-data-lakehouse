"""
R2 SQL HTTP REST API client for Cloudflare R2.

NOTE: The endpoint below reflects our best understanding of the Cloudflare
R2 SQL API at time of writing. Cloudflare may change it — check the
official docs if requests fail:
https://developers.cloudflare.com/r2/api/
"""

from __future__ import annotations

import argparse
import os

import pandas as pd
import requests
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Raised when the API returns a 401 Unauthorized response."""


class QueryError(Exception):
    """Raised when the API returns a SQL / request error (400, 422)."""


class RateLimitError(Exception):
    """Raised when the API returns 429 Too Many Requests."""


class ConnectionError(Exception):  # noqa: A001 — intentional shadow for clarity
    """Raised on network-level failures."""


# ---------------------------------------------------------------------------
# R2SQLClient
# ---------------------------------------------------------------------------

_CF_SQL_ENDPOINT = (
    "https://api.cloudflare.com/client/v4/accounts/{account_id}/analytics/r2/sql"
)


class R2SQLClient:
    """Thin Python wrapper around the Cloudflare R2 SQL HTTP REST API.

    Parameters
    ----------
    account_id:
        Cloudflare account ID.  Falls back to ``CF_ACCOUNT_ID`` env var.
    api_token:
        Cloudflare API token with R2 read permissions.
        Falls back to ``CF_API_TOKEN`` env var.
    database:
        R2 catalog / database name.  Falls back to ``R2_CATALOG_NAME`` env var.
    timeout:
        Request timeout in seconds (default 30).
    """

    def __init__(
        self,
        account_id: str | None = None,
        api_token: str | None = None,
        database: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.account_id = account_id or os.environ.get("CF_ACCOUNT_ID", "")
        self.api_token = api_token or os.environ.get("CF_API_TOKEN", "")
        self.database = database or os.environ.get("R2_CATALOG_NAME", "")
        self.timeout = timeout

        if not self.account_id:
            raise ValueError(
                "account_id is required. Set CF_ACCOUNT_ID in .env or pass it explicitly."
            )
        if not self.api_token:
            raise ValueError(
                "api_token is required. Set CF_API_TOKEN in .env or pass it explicitly."
            )
        if not self.database:
            raise ValueError(
                "database is required. Set R2_CATALOG_NAME in .env or pass it explicitly."
            )

        self._url = _CF_SQL_ENDPOINT.format(account_id=self.account_id)
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post(self, sql: str) -> dict:
        """Send a SQL query and return the raw response dict."""
        payload = {"sql": sql, "database": self.database}
        try:
            resp = self._session.post(self._url, json=payload, timeout=self.timeout)
        except requests.exceptions.ConnectionError as exc:
            raise ConnectionError(
                f"Network error reaching Cloudflare R2 SQL API: {exc}"
            ) from exc
        except requests.exceptions.Timeout as exc:
            raise ConnectionError(
                f"Request timed out after {self.timeout}s: {exc}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise ConnectionError(f"Unexpected request error: {exc}") from exc

        if resp.status_code == 401:
            raise AuthError("Invalid API token. Check CF_API_TOKEN in .env")
        if resp.status_code == 429:
            raise RateLimitError("Rate limit hit. Wait and retry.")
        if resp.status_code in (400, 422):
            try:
                body = resp.json()
            except Exception:
                body = {"errors": [resp.text]}
            raise QueryError(f"SQL error: {body.get('errors', resp.text)}")
        resp.raise_for_status()

        return resp.json()

    @staticmethod
    def _results_to_df(result: list[dict]) -> pd.DataFrame:
        if not result:
            return pd.DataFrame()
        return pd.DataFrame(result)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query_raw(self, sql: str) -> dict:
        """Execute *sql* and return the raw JSON response dict."""
        return self._post(sql)

    def query(self, sql: str, max_rows: int = 10000) -> pd.DataFrame:
        """Execute *sql* and return results as a :class:`pandas.DataFrame`.

        If *max_rows* is set and the query does not already contain a
        ``LIMIT`` clause, one is appended automatically.
        """
        if max_rows and "limit" not in sql.lower():
            sql = f"{sql.rstrip().rstrip(';')} LIMIT {max_rows}"

        data = self._post(sql)
        result = data.get("result", [])
        return self._results_to_df(result)

    def list_tables(self) -> list[str]:
        """Return a list of all table names in the catalog."""
        # Try SHOW TABLES first; fall back to information_schema if needed.
        try:
            df = self.query("SHOW TABLES", max_rows=0)
            # Column name varies by engine — grab the first column.
            return df.iloc[:, 0].tolist()
        except QueryError:
            df = self.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
                "ORDER BY table_name",
                max_rows=0,
            )
            return df["table_name"].tolist()

    def describe_table(self, table: str) -> pd.DataFrame:
        """Return the schema of *table* as a DataFrame.

        Columns: ``column_name``, ``data_type``, ``nullable``.
        """
        try:
            raw = self.query(f"DESCRIBE {table}", max_rows=0)
            # Normalise to the expected column names.
            rename_map: dict[str, str] = {}
            cols_lower = {c.lower(): c for c in raw.columns}
            for target in ("column_name", "data_type", "nullable"):
                for candidate in (target, target.replace("_", ""), f"col_{target}"):
                    if candidate in cols_lower:
                        rename_map[cols_lower[candidate]] = target
                        break
            return raw.rename(columns=rename_map)
        except QueryError:
            return self.query(
                f"SELECT column_name, data_type, is_nullable AS nullable "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{table}' "
                f"ORDER BY ordinal_position",
                max_rows=0,
            )

    def sample(self, table: str, n: int = 5) -> pd.DataFrame:
        """Return the first *n* rows of *table* as a quick data preview."""
        return self.query(f"SELECT * FROM {table} LIMIT {n}", max_rows=0)

    def get_date_range(self, table: str, date_col: str = "date") -> dict:
        """Return ``min_date``, ``max_date``, and ``row_count`` for *table*."""
        df = self.query(
            f"SELECT MIN({date_col}) AS min_date, "
            f"MAX({date_col}) AS max_date, "
            f"COUNT(*) AS row_count "
            f"FROM {table}",
            max_rows=0,
        )
        if df.empty:
            return {"min_date": None, "max_date": None, "row_count": 0}
        row = df.iloc[0]
        return {
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "row_count": int(row["row_count"]),
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_df(df: pd.DataFrame) -> None:
    print(tabulate(df.values, headers=list(df.columns), tablefmt="github"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query Cloudflare R2 SQL from the command line."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", metavar="QUERY", help="SQL query to execute")
    group.add_argument(
        "--list-tables", action="store_true", help="List all available tables"
    )
    group.add_argument("--describe", metavar="TABLE", help="Describe a table's schema")

    parser.add_argument(
        "--max-rows",
        type=int,
        default=10000,
        help="Maximum rows returned (default: 10000). Use 0 for no limit.",
    )
    args = parser.parse_args()

    client = R2SQLClient()

    if args.sql:
        df = client.query(args.sql, max_rows=args.max_rows)
        _print_df(df)
    elif args.list_tables:
        tables = client.list_tables()
        for t in tables:
            print(t)
    elif args.describe:
        df = client.describe_table(args.describe)
        _print_df(df)


if __name__ == "__main__":
    main()
