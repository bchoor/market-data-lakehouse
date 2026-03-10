# EP 2 — "2 Years of Free Stock Data in 5 Minutes — yfinance + Pre-Computed Indicators"
## Teleprompter Script · The $0 Market Data Stack

---

**Target duration:** ~15 min
**Spoken word target:** ~1,800–2,000 words
**Format key:**
- `[SCREEN: ...]` — what to show on screen
- `[ACTION: ...]` — command to run or UI interaction
- `[PAUSE]` — hold on screen, let output appear

---

---
[SCREEN: terminal, empty, project root]
---

Two years of daily stock data — SPY, AAPL, VIX — fully loaded into Parquet with RSI, MACD, and Bollinger Bands already computed. Five minutes. Zero API keys. That's what this episode is about.

By the end you'll have a local file-based data warehouse you can query with plain SQL. No cloud account. No subscription. Nothing to configure before you can run this.

---
[SCREEN: file tree showing ingestion/yfinance_backfill.py]
---

This is `yfinance_backfill.py`. It lives in the `ingestion/` folder of the repo. I'm going to walk through what it does, then run it live, then show you how to inspect the output with DuckDB.

---

## Section 1 — Why yfinance?

---
[SCREEN: split — yfinance GitHub page on left, blank terminal on right]
---

yfinance wraps Yahoo Finance's unofficial API. It's been around since 2017. The community is huge, it handles splits and dividends, and the data quality for daily OHLCV is genuinely good.

What it doesn't give you: intraday below 60-day history, Level 2 order book, options greeks. If you need those, that's a different episode. For daily equity and index data going back years, yfinance is the right tool.

Why not Alpha Vantage, Polygon, or Quandl? Those are good services. They all have rate limits on free tiers, and most require API keys. yfinance has no key. That fits the $0 constraint for this series.

One real limitation: Yahoo's terms of service don't explicitly allow commercial use. If you're building a product you sell, get proper data licensing. For backtesting, research, and personal trading — this is what most people use.

---

## Section 2 — Walking Through the Script

---
[SCREEN: ingestion/yfinance_backfill.py, top of file, lines 1–20]
---

The script has three main functions. `download_and_compute` pulls the data and calculates all the indicators. `save_parquet` writes the output split by year. And `main` handles the CLI arguments.

Let's look at the download function.

---
[SCREEN: lines 20–48, highlight `auto_adjust=False`]
---

The key flag here is `auto_adjust=False`. By default yfinance adjusts all prices for splits and dividends. When you pass `auto_adjust=False`, you get both the raw close price and the adjusted close in separate columns.

Why does that matter? Because close and adj_close diverge every time a stock pays a dividend. If you backtest a strategy using raw close prices on a dividend-paying stock, your P&L will look wrong — you'll miss income that actually happened. Use `adj_close` for any returns calculation. Keep raw `close` if you need to match against real-time quotes.

---
[SCREEN: lines 57–89, indicator block]
---

After the download, the script computes nine indicators using `pandas-ta`. All at once, before writing anything to disk.

SMA-20, SMA-50, SMA-200. EMA-12 and EMA-26. RSI-14. MACD. Bollinger Bands. ATR. OBV.

Why compute these at ingest instead of at query time? Two reasons. First, speed. When you're backtesting and running thousands of queries, you don't want to re-derive RSI from scratch on every query. It's already there. Second, consistency. If you compute RSI in three different places in your codebase, you'll eventually get three slightly different answers depending on which library and settings you used. Compute once, store once, trust the output.

In plain English: pre-computing at ingest is like mise en place in cooking. You prep everything before service starts so you're not chopping onions when orders are flying in.

---
[SCREEN: lines 98–120, rename mapping and final column list]
---

pandas-ta returns columns with ugly names like `MACDh_12_26_9`. The script renames everything to clean snake_case: `macd_hist`, `macd_signal`, `macd`. Same for Bollinger Bands and ATR.

The final column list is explicit — 24 columns, always in the same order. That's intentional. Predictable schema means downstream code doesn't break if you re-run the backfill.

---
[SCREEN: lines 133–152, save_parquet function]
---

The save function splits the data by year. Each ticker gets its own directory. Inside that directory, one Parquet file per year.

So for SPY from 2022 to 2024, you get:

```
data/ohlcv/
  SPY/
    2022.parquet
    2023.parquet
    2024.parquet
```

And for `^VIX` — notice the caret — the function sanitizes that to a filesystem-safe name. The directory will be `_VIX/` not `^VIX/`. The ticker value stored inside the file still has the caret. It's only the directory name that gets sanitized.

---

## Section 3 — Running It

---
[SCREEN: terminal, project root]
---

Let's run it. I'll use three tickers: SPY, AAPL, and `^VIX`. Date range is January 2022 through end of 2024. That's three years of daily data.

The command is:

---
[ACTION: type and run this command]
```bash
python ingestion/yfinance_backfill.py \
  --tickers SPY AAPL ^VIX \
  --start 2022-01-01 \
  --end 2024-12-31 \
  --output-dir data/ohlcv/
```
---

---
[PAUSE — wait for download to complete, show log output scrolling]
---

You can see it logging each ticker as it downloads. The format is timestamp, log level, message. Each line tells you what's happening: downloading, saving rows to a specific file.

---
[PAUSE — wait for full completion, all three tickers done]
---

Done. Three tickers, three years each. On a normal home connection that takes about 30 to 60 seconds depending on your internet speed.

One thing to call out: `^VIX`. If you type just `VIX` without the caret, Yahoo Finance won't recognize it. The caret is part of the symbol — it's how Yahoo identifies index contracts as opposed to individual stocks. You'll get an empty response and an error. Always use `^VIX` for the CBOE Volatility Index.

What is VIX? In plain English: it's the market's fear gauge. It measures how much volatility options traders are pricing in over the next 30 days. When VIX is below 15, markets are calm. When it spikes above 30, something is wrong. A lot of traders use VIX as a regime filter — run different strategies in high-vol vs. low-vol environments.

---
[ACTION: run `ls -la data/ohlcv/`]
---

---
[PAUSE]
---

Three directories: SPY, AAPL, `_VIX`. Each has three Parquet files — one per year.

---
[ACTION: run `ls -la data/ohlcv/SPY/`]
---

---
[PAUSE]
---

About 50 to 80 KB per file. Three years of daily OHLCV plus 24 columns of indicators for a single ticker takes under 250KB total. That's the power of Parquet.

Why Parquet instead of CSV? CSV stores everything as text. If you want just the `rsi_14` column, your query engine still has to read every byte in the file. Parquet is columnar — columns are stored separately on disk. Read only what you need. For a table with 24 columns, that can be a 10x to 20x speed difference. It also compresses much better than CSV.

---

## Section 4 — Inspecting with DuckDB

---
[SCREEN: terminal, project root]
---

Now let's query the data. Open DuckDB:

---
[ACTION: type `duckdb` and press enter]
---

---
[PAUSE — DuckDB prompt appears]
---

DuckDB launched with an in-memory database. No server. No config. It can read Parquet files directly with `read_parquet`.

Let's look at the last 10 rows for SPY with a few indicators:

---
[ACTION: type and run this query]
```sql
SELECT ticker, date, close, rsi_14, sma_20
FROM read_parquet('data/ohlcv/**/*.parquet')
WHERE ticker = 'SPY'
ORDER BY date DESC
LIMIT 10;
```
---

---
[PAUSE — results appear]
---

There's your output. Date descending, so you're seeing the most recent trading days first. Close price, RSI-14, and the 20-day simple moving average.

Notice the `**/*.parquet` glob pattern. That tells DuckDB to read all Parquet files across all subdirectories. It handles the year-partitioned structure automatically. You don't need to union across files manually — DuckDB does it for you.

Now let's get a summary across all three tickers:

---
[ACTION: type and run this query]
```sql
SELECT ticker, COUNT(*) as days, ROUND(AVG(rsi_14), 1) as avg_rsi, MIN(rsi_14), MAX(rsi_14)
FROM read_parquet('data/ohlcv/**/*.parquet')
GROUP BY ticker;
```
---

---
[PAUSE — results appear]
---

Three things to notice here.

First, the row count. You should see roughly 750 to 756 rows per ticker for a three-year window. The US stock market has about 252 trading days per year, not 365. Markets are closed weekends, federal holidays, and occasionally for other reasons. So three years is roughly 756 trading days. If you're building date ranges in your backtest, always use trading-day counts, not calendar days.

Second, AVG and MIN for `rsi_14`. RSI can't go below 0 or above 100 by construction. If you see values outside that range, something is wrong. For VIX, RSI is less intuitive — VIX doesn't trend the way equities do — but it still gives you a sense of whether implied volatility is elevated relative to recent history.

Third, MIN values will likely show NULL or be close to NULL. That's the warm-up period. RSI needs 14 days of data before it can produce a value. The first 14 rows in your dataset will have `NULL` for `rsi_14`. That's correct behavior, not a bug. Same for SMA-200 — the first 199 rows are NULL. If you need clean data from a specific start date, backfill starting about 10 months earlier to give all indicators time to warm up.

In plain English: indicators are like engines. They need a few rotations before they start generating power.

---

## Section 5 — Understanding the Schema

---
[SCREEN: show the 24-column list in the script, lines 114–119]
---

Let me walk through the full schema quickly. You have your price columns: date, ticker, open, high, low, close, volume, adj_close. Eight columns.

Then indicators. Moving averages: SMA-20, SMA-50, SMA-200, EMA-12, EMA-26. Momentum: RSI-14. Trend: MACD, MACD signal, MACD histogram. Volatility: the five Bollinger Band columns — upper, middle, lower, bandwidth, and percent B. ATR-14. And OBV, which is on-balance volume — a running total of volume weighted by price direction.

Quick note on adj_close vs close. If you're calculating percent returns for a backtest, always use adj_close. It's been corrected for splits and dividends. If SPY paid a $1.60 quarterly dividend and you're computing daily returns on raw close, your backtest will show a fake drop on ex-dividend date. adj_close smooths that out.

When do you use raw close? When you're comparing to real-time price feeds, which typically give you the unadjusted last price. Or when you're displaying prices to a user — people expect to see the actual traded price, not a retroactively adjusted one.

---

## Outro

---
[SCREEN: terminal, file tree showing data/ohlcv/]
---

You now have a local Parquet warehouse with two-plus years of market data and pre-computed indicators. No API keys. No cloud bills. Queryable with SQL in under a second.

In EP 3, we wire this up to an MCP server so an LLM can query your data directly. See you there.

---

*End of script.*
