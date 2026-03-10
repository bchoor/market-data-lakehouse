"""Schema introspection tools for the market-data MCP server."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from cloudflare.r2sql_client import R2SQLClient


def get_table_schema(table_name: str) -> tuple[str, str]:
    """Return schema and sample rows for *table_name*.

    Parameters
    ----------
    table_name:
        Name of the table to inspect.

    Returns
    -------
    tuple[str, str]
        A 2-tuple of (schema_markdown, sample_markdown).
        Either string may be empty if no data is available.
    """
    client = R2SQLClient()
    schema_df = client.describe_table(table_name)
    schema_md = schema_df.to_markdown(index=False) if not schema_df.empty else ""

    sample_df = client.sample(table_name, 3)
    sample_md = sample_df.to_markdown(index=False) if not sample_df.empty else ""

    return schema_md, sample_md


def get_table_date_range(table_name: str) -> dict:
    """Return ``min_date``, ``max_date``, and ``row_count`` for *table_name*.

    Parameters
    ----------
    table_name:
        Name of the table to inspect.

    Returns
    -------
    dict
        Keys: ``min_date``, ``max_date``, ``row_count``.
    """
    client = R2SQLClient()
    return client.get_date_range(table_name)
