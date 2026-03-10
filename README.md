# Market Data Lakehouse

A $0–$50/month market data stack for indie hackers and algo traders. Built on Cloudflare R2 + R2 Data Catalog + R2 SQL, with an AI-native query interface via Claude MCP.

## Stack

- **Storage**: Cloudflare R2 (S3-compatible)
- **Format**: Apache Iceberg (managed by R2 Data Catalog)
- **Query**: R2 SQL (HTTP REST API) + DuckDB (local dev)
- **AI Interface**: Claude MCP server + Claude Code skill
- **Data Sources**: yfinance (free), Alpaca Markets (free), Alpha Vantage ($50/mo)

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials
2. Install dependencies: `pip install -e .`
3. Initialize R2 + Data Catalog: `python cloudflare/catalog_setup.py --init`
4. Run backfill: `python ingestion/incremental_runner.py --mode backfill --tickers SPY AAPL --underlyings SPY`

## Episode Guide

| EP | Script | Description |
|----|--------|-------------|
| 2 | `ingestion/yfinance_backfill.py` | OHLCV + technical indicators |
| 3 | `ingestion/alpaca_options_backfill.py` | Options chains (2yr history) |
| 4 | `ingestion/greeks_calculator.py` | Black-Scholes greeks |
| 5 | `ingestion/incremental_runner.py` | Daily update pipeline |
| 6 | `cloudflare/catalog_setup.py` | R2 + Data Catalog setup |
| 7 | `cloudflare/iceberg_writer.py` | Write to R2 Iceberg |
| 8 | `cloudflare/r2sql_client.py` | R2 SQL HTTP client |
| 9 | `cloudflare/duckdb_local.py` | Local DuckDB dev layer |
| 10 | `mcp/server.py` | Claude MCP server |
| 11 | `skills/query-market.md` | Claude Code skill |
