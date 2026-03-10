# The $0 Market Data Stack

**Bloomberg-quality data infrastructure for indie hackers and algo traders**

---

## Who This Is For

This series is for developers who trade — or want to. You can run Python, read code, and you're tired of paying $25k/year for Bloomberg or settling for whatever Yahoo Finance gives you. You don't need a quant PhD. You need a practical, production-grade market data pipeline you can actually own, extend, and query with plain English. By the end of this series you'll have a real options-aware data lakehouse running on Cloudflare's free tier, queryable via SQL or natural language through Claude.

---

## What You'll Build

```
┌──────────────────────────────────────────────────────────────────────┐
│                      THE $0 MARKET DATA STACK                        │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   DATA SOURCES              INGESTION              STORAGE           │
│  ─────────────            ────────────           ─────────           │
│  yfinance (free)    ───►  yfinance_backfill ───► Parquet files       │
│  Alpaca (free)      ───►  alpaca_options   ───►  (local / R2)        │
│  Alpha Vantage ($)  ───►  alpha_vantage    ─┐                        │
│  py_vollib (local)  ───►  greeks_calc      ─┘                        │
│                           incremental_runner                         │
│                                │                                     │
│                                ▼                                     │
│                     ┌──────────────────┐                             │
│   LAKEHOUSE         │  Cloudflare R2   │  (S3-compatible object      │
│  ──────────         │  + Data Catalog  │   store, ~free)             │
│                     │  + R2 SQL        │                             │
│                     │  Apache Iceberg  │  (open table format)        │
│                     └────────┬─────────┘                             │
│                              │                                       │
│                    ┌─────────┴──────────┐                            │
│   QUERY LAYER      │                    │                            │
│  ────────────      │  R2 SQL (remote)   │  DuckDB (local dev)        │
│                    │  r2sql_client.py   │  duckdb_local.py           │
│                    └─────────┬──────────┘                            │
│                              │                                       │
│                    ┌─────────▼──────────┐                            │
│   AI INTERFACE     │    MCP Server      │  Claude Desktop /          │
│  ─────────────     │    server.py       │  Claude Code               │
│                    │    /query-market   │  skill                     │
│                    └────────────────────┘                            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11+ | The scripts use modern type hints and match-case |
| Cloudflare account | Free tier is sufficient through EP 9 |
| Basic pandas knowledge | `df.groupby`, `df.merge`, reading DataFrames |
| Git + terminal comfort | We run commands, not notebooks |

---

## Cost Breakdown

| Component | Free Tier | Paid Upgrade |
|---|---|---|
| yfinance | Free (Yahoo Finance) | — |
| Alpaca Markets | Free (delayed + current snapshots) | $9/mo for live data |
| Cloudflare R2 | 10 GB storage, 1M Class A ops/mo | ~$0.015/GB-mo after |
| Cloudflare R2 SQL | Included with R2 | Usage-based above free |
| Alpha Vantage | 25 API calls/day | $50/mo (Premium) |
| py_vollib (Greeks) | Free (local compute) | — |
| **Total (free path)** | **$0/mo** | — |
| **Total (paid path)** | — | **~$50/mo** |

---

## Data Sources

| Source | What It Provides | Tier | Episode |
|---|---|---|---|
| yfinance | OHLCV, splits, dividends for any ticker | Free | EP 2 |
| Alpaca Markets | Options chains (current snapshots), live quotes | Free / $9 | EP 3 |
| py_vollib | Black-Scholes Greeks computed locally | Free (local) | EP 4 |
| Alpha Vantage | Clean historical options data, greeks, more history | $50/mo | EP 13 |

---

## Episode Index

| EP | Title | Deliverable | Duration | Difficulty | Track |
|---|---|---|---|---|---|
| 01 | Architecture & CF Data Platform | Full stack mental model | ~15 min | Beginner | Foundation |
| 02 | OHLCV Backfill with yfinance | Parquet files of price + indicators | ~15 min | Beginner | Foundation |
| 03 | Options Chains with Alpaca | Snapshot options chain Parquet files | ~15 min | Intermediate | Foundation |
| 04 | Computing Greeks with py_vollib | Options + Greeks merged dataset | ~12 min | Intermediate | Foundation |
| 05 | Incremental Pipeline | Idempotent scheduled runner with checkpoints | ~18 min | Intermediate | Foundation |
| 06 | R2 + Data Catalog Setup | Live Cloudflare R2 bucket + Iceberg catalog | ~15 min | Intermediate | CF Platform |
| 07 | Writing to Iceberg | Data in R2 as queryable Iceberg tables | ~20 min | Advanced | CF Platform |
| 08 | Querying with R2 SQL | Working SQL screener against live lakehouse | ~15 min | Intermediate | CF Platform |
| 09 | DuckDB Local Dev Layer | Full query stack running locally for free | ~12 min | Beginner/Int | CF Platform |
| 10 | MCP Server | Claude talking to your lakehouse | ~20 min | Advanced | AI Interface |
| 11 | Claude Code Skill | `/query-market` skill wired to live data | ~10 min | Beginner | AI Interface |
| 12 | Live Demo: Natural Language → SQL | Full end-to-end workflow on real queries | ~20 min | All levels | AI Interface |
| 13 | Alpha Vantage Upgrade Path | Cleaner historical options data pipeline | ~12 min | Intermediate | Advanced |

---

## How to Follow Along

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/market-data-lakehouse
cd market-data-lakehouse

# 2. Copy environment variables
cp .env.example .env
# Edit .env — fill in your Cloudflare and Alpaca credentials

# 3. Install in editable mode
pip install -e .

# 4. Verify
python -c "import ingestion; print('ready')"
```

Each episode refers to the same repo. The branch in each episode guide matches the state of the code at that point.

---

## Tracks

**Foundation (EP 1–5):** Local pipeline. No cloud required. Data lives on disk.

**CF Platform (EP 6–9):** Push to Cloudflare. Data lives in R2 as an Iceberg lakehouse.

**AI Interface (EP 10–12):** Wire Claude into the lakehouse. Query with natural language.

**Advanced (EP 13):** Upgrade data quality when you're trading real size.
