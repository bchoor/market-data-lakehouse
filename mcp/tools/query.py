"""Query tool implementation for the market-data MCP server."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from cloudflare.r2sql_client import R2SQLClient


def execute_query(sql: str, max_rows: int = 1000) -> tuple[str, int]:
    """Execute SQL and return (markdown_table, row_count).

    Parameters
    ----------
    sql:
        SQL query string to execute against the R2 lakehouse.
    max_rows:
        Maximum number of rows to return (default 1000).

    Returns
    -------
    tuple[str, int]
        A 2-tuple of (markdown_table_string, row_count).
        Returns ("", 0) when the query produces no rows.
    """
    client = R2SQLClient()
    df = client.query(sql, max_rows=max_rows)
    if df.empty:
        return "", 0
    return df.to_markdown(index=False), len(df)
