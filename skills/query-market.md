---
name: query-market
description: Query the market data lakehouse via natural language. Translates user intent into R2 SQL queries against OHLCV, options chain, and options greeks tables.
---

# Market Data Query Skill

This skill teaches Claude how to translate natural language market data questions into SQL queries against the R2 SQL lakehouse. When a user asks about stock prices, technical indicators, options flow, implied volatility, or greeks, Claude selects the right table, builds a correct SQL query, executes it via the `query_market_data` MCP tool, and interprets the results in plain English.

The lakehouse is backed by Cloudflare R2 + R2 Data Catalog (Apache Iceberg format) and exposed via the `market-data` MCP server. All queries go through the `query_market_data(sql)` tool unless the user explicitly asks to see the SQL without running it.

---

## Schema Reference

### Table: `ohlcv`

Daily OHLCV prices and pre-computed technical indicators. One row per ticker per trading day. Source: yfinance (unadjusted prices) + pandas-ta indicators.

> **Ticker name convention**: The caret character `^` is replaced with `_` in file-system paths (e.g., the directory is stored as `_VIX`), but the **`ticker` column stores the original symbol** — query using `ticker = '^VIX'`, not `'_VIX'`.

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Trading date (exchange calendar, no weekends/holidays) |
| ticker | STRING | Ticker symbol — use original form, e.g., `SPY`, `AAPL`, `^VIX` |
| open | DOUBLE | Opening price (unadjusted) |
| high | DOUBLE | Daily high (unadjusted) |
| low | DOUBLE | Daily low (unadjusted) |
| close | DOUBLE | Closing price (unadjusted) |
| volume | BIGINT | Trading volume (shares) |
| adj_close | DOUBLE | Split/dividend-adjusted close (use for return calculations) |
| sma_20 | DOUBLE | 20-day simple moving average of close; NaN for first 19 rows per ticker |
| sma_50 | DOUBLE | 50-day simple moving average of close; NaN for first 49 rows |
| sma_200 | DOUBLE | 200-day simple moving average of close; NaN for first 199 rows |
| ema_12 | DOUBLE | 12-day exponential moving average of close |
| ema_26 | DOUBLE | 26-day exponential moving average of close |
| rsi_14 | DOUBLE | 14-day RSI (0–100). Below 30 = oversold, above 70 = overbought |
| macd | DOUBLE | MACD line = ema_12 − ema_26 |
| macd_signal | DOUBLE | Signal line = 9-day EMA of macd |
| macd_hist | DOUBLE | Histogram = macd − macd_signal. Positive = bullish momentum |
| bb_upper | DOUBLE | Bollinger Band upper = sma_20 + 2 standard deviations |
| bb_middle | DOUBLE | Bollinger Band middle = sma_20 |
| bb_lower | DOUBLE | Bollinger Band lower = sma_20 − 2 standard deviations |
| bb_bandwidth | DOUBLE | Band width = (bb_upper − bb_lower) / bb_middle. High = volatile |
| bb_pct_b | DOUBLE | %B: price position within bands. 0 = at lower band, 1 = at upper band |
| atr_14 | DOUBLE | 14-day Average True Range — absolute volatility in price units |
| obv | DOUBLE | On-Balance Volume — cumulative volume flow; trend-confirms price |

---

### Table: `options`

Daily end-of-day options chain snapshots. One row per underlying × date × expiration × strike × option_type. Source: Alpaca Markets (2 years of history).

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Snapshot date (when the data was captured) |
| underlying | STRING | Underlying ticker symbol, e.g., `SPY`, `AAPL` |
| symbol | STRING | OCC option symbol, e.g., `SPY240119C00450000` |
| expiration | DATE | Option expiration date |
| strike | DOUBLE | Strike price |
| option_type | STRING | `call` or `put` |
| bid | DOUBLE | Best bid price |
| ask | DOUBLE | Best ask price |
| mid_price | DOUBLE | Midpoint = (bid + ask) / 2 |
| implied_volatility | DOUBLE | Implied volatility as a decimal (0.20 = 20% IV) |
| volume | BIGINT | Contracts traded on `date` (nullable) |
| open_interest | BIGINT | Open contracts as of `date` (nullable) |
| in_the_money | BOOLEAN | True if the option is in-the-money at close on `date` |

---

### Table: `options_greeks`

Options chain with Black-Scholes greeks computed via py_vollib. Contains all columns from `options` plus five greek columns. Rows where greeks could not be computed (e.g., missing spot price or IV = 0) have NaN for all greek columns.

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Snapshot date |
| underlying | STRING | Underlying ticker symbol |
| symbol | STRING | OCC option symbol |
| expiration | DATE | Option expiration date |
| strike | DOUBLE | Strike price |
| option_type | STRING | `call` or `put` |
| bid | DOUBLE | Best bid price |
| ask | DOUBLE | Best ask price |
| mid_price | DOUBLE | Midpoint price |
| implied_volatility | DOUBLE | Implied volatility as a decimal |
| volume | BIGINT | Contracts traded on `date` |
| open_interest | BIGINT | Open contracts as of `date` |
| in_the_money | BOOLEAN | True if in-the-money |
| delta | DOUBLE | Rate of change of option price w.r.t. underlying price. Calls: 0–1, Puts: −1–0 |
| gamma | DOUBLE | Rate of change of delta w.r.t. underlying price. Always positive |
| theta | DOUBLE | Daily time decay in dollars per contract. Always negative |
| vega | DOUBLE | Sensitivity to 1% change in IV (in dollars per contract) |
| rho | DOUBLE | Sensitivity to 1% change in risk-free rate |

---

## Query Templates

### 1. Stock Screener — Filter by RSI, MACD, Volume

**When to use**: User wants to find tickers meeting technical conditions (e.g., "oversold stocks", "MACD bullish crossover", "high-volume breakouts").

**Natural language triggers**:
- "Which stocks are oversold?"
- "Find tickers with RSI below 30 this week"
- "Show me MACD bullish crossovers"
- "Stocks above their 200-day moving average with high volume"

```sql
-- Stock screener: RSI oversold + MACD bullish histogram on the most recent trading day
-- Adjust thresholds and conditions to match the user's criteria
SELECT
    ticker,
    date,
    close,
    rsi_14,
    macd,
    macd_hist,
    macd_signal,
    volume,
    sma_50,
    sma_200,
    -- % above/below 200-day MA
    ROUND((close - sma_200) / sma_200 * 100, 2) AS pct_from_sma200
FROM ohlcv
WHERE
    -- Most recent available date for all tickers
    date = (SELECT MAX(date) FROM ohlcv)
    -- RSI oversold threshold (adjust: <30 oversold, >70 overbought)
    AND rsi_14 < 30
    -- MACD histogram turning positive (bullish momentum)
    AND macd_hist > 0
    -- Minimum liquidity filter
    AND volume > 1000000
ORDER BY rsi_14 ASC
LIMIT 25;
```

**Example result**: Returns a ranked list of tickers sorted by most oversold RSI, with their current MACD momentum and volume context.

---

### 2. Options Flow — High Volume / Unusual Activity

**When to use**: User wants to find options with unusual volume relative to open interest, suggesting institutional flow or directional bets.

**Natural language triggers**:
- "Unusual options activity on AAPL"
- "High volume options today"
- "Show me options where volume exceeds open interest"
- "What are the big bets in SPY options?"

```sql
-- Unusual options flow: volume-to-open-interest ratio > 1 on the latest snapshot date
-- High vol/OI ratio = new positions being opened aggressively (unusual activity signal)
SELECT
    underlying,
    date,
    expiration,
    strike,
    option_type,
    bid,
    ask,
    mid_price,
    volume,
    open_interest,
    -- Ratio > 1 means more contracts traded than currently exist (strong activity signal)
    ROUND(CAST(volume AS DOUBLE) / NULLIF(open_interest, 0), 2) AS vol_oi_ratio,
    implied_volatility,
    in_the_money
FROM options
WHERE
    underlying = 'AAPL'  -- replace with target ticker
    AND date = (SELECT MAX(date) FROM options)
    AND volume > 500      -- minimum volume filter to exclude noise
    AND open_interest > 0
    AND CAST(volume AS DOUBLE) / open_interest > 1.0  -- unusual: more volume than OI
ORDER BY vol_oi_ratio DESC
LIMIT 20;
```

**Example result**: Returns the most aggressively traded options contracts, sorted by volume/OI ratio. Calls vs puts split reveals directional bias.

---

### 3. Volatility Surface — IV by Strike and Expiry

**When to use**: User wants to see the full implied volatility surface (smile/skew) for an underlying across all strikes and expirations.

**Natural language triggers**:
- "Show me the volatility surface for SPY"
- "What does the IV skew look like for TSLA?"
- "Options IV by strike and expiry for QQQ"
- "Volatility smile for AAPL calls"

```sql
-- Implied volatility surface: IV organized by expiration and strike
-- Shows the volatility smile/skew shape across the term structure
SELECT
    underlying,
    expiration,
    strike,
    option_type,
    implied_volatility,
    mid_price,
    volume,
    open_interest,
    in_the_money,
    -- Days to expiration for term-structure context
    DATEDIFF('day', date, expiration) AS dte
FROM options
WHERE
    underlying = 'SPY'  -- replace with target ticker
    AND date = (SELECT MAX(date) FROM options WHERE underlying = 'SPY')
    -- Optional: filter to specific option type
    -- AND option_type = 'put'
    -- Optional: filter to expirations within next 60 days
    -- AND expiration <= CURRENT_DATE + INTERVAL 60 DAY
    AND implied_volatility IS NOT NULL
    AND implied_volatility > 0
ORDER BY expiration, strike
LIMIT 200;
```

**Example result**: A grid of IV values by strike/expiry. The volatility smile is visible — IV typically rises for deep OTM puts (skew) and can show humps near earnings dates.

---

### 4. Price History — OHLCV with Technical Indicators

**When to use**: User wants a time series of price data and indicators for one ticker over a date range.

**Natural language triggers**:
- "Show me AAPL's price history for the last 3 months"
- "SPY OHLCV with RSI and MACD for 2024"
- "Give me NVDA's Bollinger Bands over the last 60 days"
- "What is QQQ's moving average status?"

```sql
-- Price history with full technical indicator suite for one ticker
-- Adjust ticker and date range as needed
SELECT
    date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    -- Trend indicators
    sma_20,
    sma_50,
    sma_200,
    -- Momentum
    rsi_14,
    macd,
    macd_signal,
    macd_hist,
    -- Volatility / bands
    bb_upper,
    bb_middle,
    bb_lower,
    bb_bandwidth,
    bb_pct_b,
    atr_14,
    -- Volume flow
    obv,
    -- Derived: close vs key MAs (positive = above MA)
    ROUND(close - sma_50, 2)  AS dist_from_sma50,
    ROUND(close - sma_200, 2) AS dist_from_sma200
FROM ohlcv
WHERE
    ticker = 'SPY'  -- replace with target ticker
    AND date >= CURRENT_DATE - INTERVAL 90 DAY  -- adjust lookback
ORDER BY date ASC;
```

**Example result**: A time series suitable for charting or analysis. Includes all pre-computed indicators so no additional calculation is needed.

---

### 5. Top Movers — Biggest % Change in a Day

**When to use**: User wants to see which tickers had the largest price moves on a given day.

**Natural language triggers**:
- "What were the biggest movers today?"
- "Top gainers and losers this week"
- "Which tickers moved more than 5% yesterday?"
- "Show me the most volatile stocks on 2024-03-15"

```sql
-- Top movers by daily percentage change
-- Uses adj_close for accurate % change (accounts for splits/dividends)
WITH latest AS (
    -- Get the two most recent trading dates available
    SELECT DISTINCT date FROM ohlcv ORDER BY date DESC LIMIT 2
),
ranked AS (
    SELECT
        o.ticker,
        o.date,
        o.close,
        o.adj_close,
        o.volume,
        LAG(o.adj_close) OVER (PARTITION BY o.ticker ORDER BY o.date) AS prev_adj_close
    FROM ohlcv o
    WHERE o.date IN (SELECT date FROM latest)
)
SELECT
    ticker,
    date,
    close,
    ROUND(prev_adj_close, 2)                                   AS prev_close,
    ROUND((adj_close - prev_adj_close) / prev_adj_close * 100, 2) AS pct_change,
    volume
FROM ranked
WHERE
    date = (SELECT MAX(date) FROM latest)
    AND prev_adj_close IS NOT NULL
    AND prev_adj_close > 0
ORDER BY ABS((adj_close - prev_adj_close) / prev_adj_close) DESC
LIMIT 20;
```

**Example result**: Ranked list of tickers by absolute % change. Positive pct_change = gainers, negative = losers. Split between top gainers and losers by changing the ORDER BY.

---

### 6. IV Rank / Percentile — Historical IV Context

**When to use**: User wants to know whether current implied volatility is historically high or low for an underlying (IV rank or IV percentile).

**Natural language triggers**:
- "What is the IV rank for SPY?"
- "Is AAPL's implied volatility high or low historically?"
- "IV percentile for QQQ vs IWM"
- "Compare IV context across my watchlist"

```sql
-- IV Rank: how does current avg IV compare to its 1-year range?
-- IV Rank = (current_iv - min_iv) / (max_iv - min_iv) × 100
-- IV Rank 0 = at 1yr low, 100 = at 1yr high; >50 = elevated, consider selling premium
WITH iv_history AS (
    SELECT
        underlying,
        date,
        -- Average IV across all strikes/expirations for each day
        AVG(implied_volatility) AS avg_iv
    FROM options
    WHERE
        underlying IN ('SPY', 'QQQ', 'IWM')  -- replace with your watchlist
        AND date >= CURRENT_DATE - INTERVAL 365 DAY
        AND implied_volatility > 0
    GROUP BY underlying, date
),
iv_range AS (
    SELECT
        underlying,
        MIN(avg_iv)  AS iv_52w_low,
        MAX(avg_iv)  AS iv_52w_high,
        -- Current IV = most recent date's average
        MAX(CASE WHEN date = (SELECT MAX(date) FROM iv_history h2
                              WHERE h2.underlying = iv_history.underlying)
                 THEN avg_iv END) AS current_iv
    FROM iv_history
    GROUP BY underlying
)
SELECT
    underlying,
    ROUND(current_iv * 100, 1)   AS current_iv_pct,
    ROUND(iv_52w_low * 100, 1)   AS iv_52w_low_pct,
    ROUND(iv_52w_high * 100, 1)  AS iv_52w_high_pct,
    ROUND(
        (current_iv - iv_52w_low) / NULLIF(iv_52w_high - iv_52w_low, 0) * 100,
        1
    ) AS iv_rank
FROM iv_range
ORDER BY iv_rank DESC;
```

**Example result**: IV rank for each underlying on a 0–100 scale. IV rank > 50 means IV is in the upper half of its 1-year range — historically favorable for premium-selling strategies.

---

### 7. Options Chain Snapshot — All Strikes for an Expiry

**When to use**: User wants to see all available strikes for a specific underlying and expiration date, including greeks.

**Natural language triggers**:
- "Show me the SPY options chain for March expiration"
- "All AAPL calls and puts expiring next Friday"
- "Options chain with greeks for TSLA 2025-01-17 expiry"
- "What are the delta values for SPY puts expiring this month?"

```sql
-- Full options chain snapshot with greeks for a specific underlying and expiration
-- Use options_greeks for delta/gamma/theta/vega/rho; use options if greeks not needed
SELECT
    underlying,
    date,
    expiration,
    strike,
    option_type,
    bid,
    ask,
    mid_price,
    implied_volatility,
    volume,
    open_interest,
    in_the_money,
    -- Black-Scholes greeks
    ROUND(delta, 4)  AS delta,
    ROUND(gamma, 4)  AS gamma,
    ROUND(theta, 4)  AS theta,   -- daily decay, negative value
    ROUND(vega, 4)   AS vega,
    ROUND(rho, 4)    AS rho
FROM options_greeks
WHERE
    underlying = 'SPY'
    AND date = (SELECT MAX(date) FROM options_greeks WHERE underlying = 'SPY')
    AND expiration = '2025-03-21'  -- replace with target expiration (YYYY-MM-DD)
    -- Optional: filter to one side
    -- AND option_type = 'put'
ORDER BY strike ASC;
```

**Example result**: A complete options chain sorted by strike, showing both calls and puts side-by-side with pricing and all five greeks. Delta moves from ~1.0 (deep ITM calls) toward 0.0 (deep OTM calls) as strike increases.

---

## How to Use This Skill

When this skill is active, follow these steps for every market data question:

1. **Identify intent** — determine whether the user wants price/indicator data (`ohlcv`), options pricing/flow (`options`), or options with greeks (`options_greeks`). If unclear, ask.

2. **Select the template** — match the user's question to one of the seven templates above, or combine them for compound questions.

3. **Fill in parameters** — substitute the correct ticker(s), date range, expiration, thresholds, and any other variables into the template. Always use uppercase tickers. Use `^VIX` (not `_VIX`) for VIX.

4. **Execute or display** — by default, call `query_market_data(sql)` via the MCP tool to run the query and return results. If the user says "show me the SQL" or "don't run it", display the formatted SQL instead.

5. **Interpret results** — after returning the data table, add a 2–4 sentence plain-English interpretation. What does the data mean? Is RSI oversold? Is IV rank elevated? Are there notable outliers?

6. **Handle no results gracefully** — if the query returns no rows, suggest why (wrong date, ticker not in dataset, IV = 0 rows filtered out) and offer to adjust the query.

7. **Use helper tools when needed**:
   - `list_tables()` — check data freshness before answering "how recent is the data?"
   - `describe_table(name)` — verify column names if a query fails
   - `get_date_range(name)` — find earliest/latest available date for a table

---

## Natural Language to SQL Examples

| User asks | SQL generated |
|-----------|---------------|
| "What is SPY's RSI today?" | `SELECT date, close, rsi_14 FROM ohlcv WHERE ticker = 'SPY' ORDER BY date DESC LIMIT 1` |
| "Show me the last 30 days of AAPL prices" | `SELECT date, open, high, low, close, volume FROM ohlcv WHERE ticker = 'AAPL' ORDER BY date DESC LIMIT 30` |
| "Which tickers have RSI below 30 right now?" | Template 1 with `rsi_14 < 30`, most recent date |
| "Is QQQ above its 200-day moving average?" | `SELECT date, close, sma_200, close > sma_200 AS above_200ma FROM ohlcv WHERE ticker = 'QQQ' ORDER BY date DESC LIMIT 1` |
| "Show me NVDA's MACD for the last 3 months" | `SELECT date, close, macd, macd_signal, macd_hist FROM ohlcv WHERE ticker = 'NVDA' AND date >= CURRENT_DATE - INTERVAL 90 DAY ORDER BY date` |
| "What is VIX today?" | `SELECT date, close, rsi_14 FROM ohlcv WHERE ticker = '^VIX' ORDER BY date DESC LIMIT 1` |
| "Top 10 biggest movers today" | Template 5 with LIMIT 10 |
| "Show unusual options activity on TSLA" | Template 2 with `underlying = 'TSLA'` |
| "What is the IV on SPY calls expiring in 30 days?" | `SELECT expiration, strike, implied_volatility, mid_price FROM options WHERE underlying = 'SPY' AND option_type = 'call' AND date = (SELECT MAX(date) FROM options) AND expiration BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL 30 DAY ORDER BY expiration, strike` |
| "SPY options IV rank" | Template 6 with `underlying IN ('SPY')` |
| "Show the full SPY options chain for March expiry with greeks" | Template 7 with `underlying = 'SPY'`, target expiration |
| "Which SPY puts have delta greater than 0.4?" | `SELECT expiration, strike, delta, mid_price, implied_volatility FROM options_greeks WHERE underlying = 'SPY' AND option_type = 'put' AND ABS(delta) > 0.4 AND date = (SELECT MAX(date) FROM options_greeks) ORDER BY expiration, strike` |
| "What stocks are in a Bollinger Band squeeze?" | `SELECT ticker, date, close, bb_bandwidth FROM ohlcv WHERE date = (SELECT MAX(date) FROM ohlcv) AND bb_bandwidth < 0.1 ORDER BY bb_bandwidth ASC LIMIT 20` |
| "Compare IV across SPY, QQQ, IWM" | Template 6 with all three underlyings |
| "How many days of history do I have for AAPL?" | `SELECT COUNT(*) AS trading_days, MIN(date) AS earliest, MAX(date) AS latest FROM ohlcv WHERE ticker = 'AAPL'` |

---

## Ticker Name Convention

| Scenario | Correct query form | Notes |
|----------|--------------------|-------|
| Standard equity | `ticker = 'AAPL'` | Uppercase, no prefix |
| Index with caret | `ticker = '^VIX'` | Use original `^` — NOT `_VIX` |
| ETF | `ticker = 'SPY'` | Same as equity |
| Options underlying | `underlying = 'SPY'` | Options tables use `underlying`, not `ticker` |

The `^` → `_` replacement only applies to **file-system paths** during ingestion. The `ticker` column in the database always stores the original symbol (e.g., `^VIX`, `^GSPC`).

---

## Common Mistakes to Avoid

- **Wrong column name for options**: Options tables use `underlying`, not `ticker`. OHLCV uses `ticker`.
- **Joining ohlcv to options**: Join on `ohlcv.ticker = options.underlying AND ohlcv.date = options.date`.
- **NaN indicators early in history**: SMA-200 is NaN for the first 199 rows per ticker. Add `AND sma_200 IS NOT NULL` to indicator queries if you need valid values.
- **IV as decimal vs percent**: `implied_volatility` is stored as a decimal (0.20 = 20% IV). Multiply by 100 when displaying to users.
- **Delta sign for puts**: Put delta is negative (−1 to 0). Use `ABS(delta)` or filter with `delta < -0.4` for put delta magnitude queries.
- **Volume nullable**: `volume` and `open_interest` in the options tables are nullable (`Int64`). Use `NULLIF(open_interest, 0)` in division to avoid divide-by-zero errors.

---

## Installation

### Global (available in all Claude Code sessions)

```bash
cp skills/query-market.md ~/.claude/skills/
```

### Project-scoped (available only in this project)

```bash
mkdir -p .claude/skills
cp skills/query-market.md .claude/skills/
```

After installing, type `/query-market` in Claude Code to activate this skill. Claude will confirm it is loaded and ready to translate natural language queries into SQL against the lakehouse.
