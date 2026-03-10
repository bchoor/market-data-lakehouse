"""MCP server exposing the R2 SQL market-data lakehouse to Claude and other MCP clients.

Run directly:
    python mcp_server/server.py

Or configure in Claude Desktop / Claude Code — see mcp_server/README.md.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    raise ImportError(
        "FastMCP not found. Install with: pip install mcp\n"
        "Or: pip install -e . from the project root"
    )

from cloudflare.r2sql_client import AuthError, QueryError, RateLimitError
from mcp_server.tools.query import execute_query
from mcp_server.tools.schema import get_table_date_range, get_table_schema, list_all_tables

# ---------------------------------------------------------------------------
# Server instantiation
# ---------------------------------------------------------------------------

mcp = FastMCP("market-data-lakehouse")

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

_KNOWN_TABLES = ("ohlcv", "options", "options_greeks")

_TABLE_DESCRIPTIONS = {
    "ohlcv": (
        "Daily OHLCV price bars + pre-computed technical indicators. "
        "Columns: ticker, date, open, high, low, close, volume, "
        "sma_20, sma_50, sma_200, ema_12, ema_26, rsi_14, macd, "
        "bb_upper, bb_middle, bb_lower, atr_14, obv."
    ),
    "options": (
        "Daily options chain snapshots. "
        "Columns: underlying, date, expiration, strike, option_type, "
        "bid, ask, mid_price, implied_volatility, volume, open_interest, in_the_money."
    ),
    "options_greeks": (
        "Options chain with Black-Scholes greeks. "
        "All columns from options plus: delta, gamma, theta, vega, rho."
    ),
}


@mcp.tool()
def query_market_data(sql: str) -> str:
    """Execute SQL against the market data lakehouse (R2 SQL).

    Available tables:
    - ohlcv: daily OHLCV + technical indicators (ticker, date, open/high/low/close,
      sma_20/50/200, ema_12/26, rsi_14, macd, bb_upper/middle/lower, atr_14, obv)
    - options: daily options chain snapshots (underlying, date, expiration, strike,
      option_type, bid, ask, mid_price, implied_volatility, volume, open_interest,
      in_the_money)
    - options_greeks: options + greeks (delta, gamma, theta, vega, rho)

    Returns results as a markdown table. Max 1000 rows returned.
    """
    try:
        result, row_count = execute_query(sql, max_rows=1000)
        if row_count == 0:
            return "Query returned no results."
        return f"**{row_count} rows returned**\n\n{result}"
    except AuthError as e:
        return f"Authentication error: {e}\nCheck CF_API_TOKEN in your .env file."
    except QueryError as e:
        return f"SQL error: {e}"
    except RateLimitError:
        return "Rate limit hit. Wait a moment and try again."
    except Exception as e:  # noqa: BLE001
        return f"Error: {e}"


@mcp.tool()
def list_tables() -> str:
    """List all available tables in the market data lakehouse with row counts and date ranges."""
    try:
        tables = list_all_tables()
    except AuthError as e:
        return f"Authentication error: {e}\nCheck CF_API_TOKEN in your .env file."
    except RateLimitError:
        return "Rate limit hit. Wait a moment and try again."
    except Exception as e:  # noqa: BLE001
        return f"Error listing tables: {e}"

    if not tables:
        return "No tables found in the catalog."

    lines: list[str] = ["## Market Data Lakehouse — Available Tables\n"]
    lines.append("| Table | Rows | Earliest Date | Latest Date | Description |")
    lines.append("|-------|-----:|---------------|-------------|-------------|")

    for table in tables:
        try:
            info = get_table_date_range(table)
            row_count = f"{info['row_count']:,}"
            min_date = str(info["min_date"]) if info["min_date"] is not None else "—"
            max_date = str(info["max_date"]) if info["max_date"] is not None else "—"
        except Exception:  # noqa: BLE001
            row_count = "—"
            min_date = "—"
            max_date = "—"

        description = _TABLE_DESCRIPTIONS.get(table, "")
        # Truncate long descriptions in the summary table
        short_desc = description[:80] + "…" if len(description) > 80 else description
        lines.append(f"| `{table}` | {row_count} | {min_date} | {max_date} | {short_desc} |")

    lines.append("")
    lines.append(
        "_Use `describe_table(table_name)` to see full schema and sample rows._"
    )

    return "\n".join(lines)


@mcp.tool()
def describe_table(table_name: str) -> str:
    """Get schema and sample data for a table.

    Returns column names, types, and 3 example rows.

    Parameters
    ----------
    table_name:
        One of: ohlcv, options, options_greeks
    """
    try:
        schema_md, sample_md = get_table_schema(table_name)
    except AuthError as e:
        return f"Authentication error: {e}\nCheck CF_API_TOKEN in your .env file."
    except QueryError as e:
        return f"SQL error describing `{table_name}`: {e}"
    except ValueError as e:
        return f"Invalid table name: {e}"
    except Exception as e:  # noqa: BLE001
        return f"Error: {e}"

    full_description = _TABLE_DESCRIPTIONS.get(table_name, "")
    lines: list[str] = [f"## Table: `{table_name}`\n"]

    if full_description:
        lines.append(f"{full_description}\n")

    lines.append("### Schema\n")
    if schema_md:
        lines.append(schema_md)
    else:
        lines.append("_Schema not available._")

    lines.append("\n### Sample Rows (3)\n")
    if sample_md:
        lines.append(sample_md)
    else:
        lines.append("_No rows found — table may be empty._")

    return "\n".join(lines)


@mcp.tool()
def get_date_range(table_name: str) -> str:
    """Get the date range and record count for a table.

    Useful for understanding data freshness and coverage before writing queries.

    Parameters
    ----------
    table_name:
        One of: ohlcv, options, options_greeks
    """
    try:
        info = get_table_date_range(table_name)
    except AuthError as e:
        return f"Authentication error: {e}\nCheck CF_API_TOKEN in your .env file."
    except QueryError as e:
        return f"SQL error for `{table_name}`: {e}"
    except ValueError as e:
        return f"Invalid table name: {e}"
    except Exception as e:  # noqa: BLE001
        return f"Error: {e}"

    min_date = info["min_date"] if info["min_date"] is not None else "N/A"
    max_date = info["max_date"] if info["max_date"] is not None else "N/A"
    row_count = f"{info['row_count']:,}"

    return (
        f"## Date Range — `{table_name}`\n\n"
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Earliest date | {min_date} |\n"
        f"| Latest date   | {max_date} |\n"
        f"| Total rows    | {row_count} |\n"
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
