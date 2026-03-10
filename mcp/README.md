# Market Data MCP Server

Exposes the R2 SQL market-data lakehouse to any MCP-compatible client (Claude Desktop, Claude Code, Cursor) via four tools. Ask natural language questions about stocks, ETFs, and options — Claude translates them to SQL and returns formatted results.

## What it does

The server wraps the `R2SQLClient` HTTP client and registers four tools:

| Tool | Purpose |
|------|---------|
| `query_market_data(sql)` | Execute any SQL against the lakehouse |
| `list_tables()` | Show all tables with row counts and date ranges |
| `describe_table(table_name)` | Schema + 3 sample rows for a table |
| `get_date_range(table_name)` | Earliest date, latest date, and total row count |

### Available tables

| Table | Contents |
|-------|----------|
| `ohlcv` | Daily OHLCV bars + pre-computed indicators: SMA 20/50/200, EMA 12/26, RSI 14, MACD, Bollinger Bands, ATR 14, OBV |
| `options` | Daily options chain snapshots: strike, expiration, bid/ask, implied volatility, volume, open interest |
| `options_greeks` | Options chain + Black-Scholes greeks: delta, gamma, theta, vega, rho |

---

## Setup

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in your Cloudflare credentials:

```
CF_ACCOUNT_ID=your_cloudflare_account_id
CF_API_TOKEN=your_api_token_with_r2_read_permissions
R2_CATALOG_NAME=market-data-catalog
```

### 3. Run the server

```bash
python mcp/server.py
```

---

## Client configuration

### Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "market-data": {
      "command": "python",
      "args": ["/path/to/market-data-lakehouse/mcp/server.py"],
      "env": {
        "CF_ACCOUNT_ID": "your_account_id",
        "CF_API_TOKEN": "your_api_token",
        "R2_CATALOG_NAME": "market-data-catalog"
      }
    }
  }
}
```

Location of the config file:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

### Claude Code (`.mcp.json` in project root)

```json
{
  "mcpServers": {
    "market-data": {
      "command": "python",
      "args": ["mcp/server.py"]
    }
  }
}
```

> **Note:** Claude Code reads `.env` from the directory where the MCP server runs.
> Ensure your `.env` file exists in the project root with `CF_ACCOUNT_ID`,
> `CF_API_TOKEN`, and `R2_CATALOG_NAME` set before starting the server.

Claude Code picks up `.mcp.json` automatically when you open the project. Credentials are read from `.env` via `python-dotenv`.

---

## Example queries

Ask Claude in natural language — it will call the right tool and SQL automatically.

### Price & technicals

```
What is SPY's RSI over the last 30 days?
→ SELECT date, close, rsi_14 FROM ohlcv
  WHERE ticker = 'SPY' ORDER BY date DESC LIMIT 30

Which tickers have RSI below 30 (oversold) this week?
→ SELECT ticker, date, close, rsi_14 FROM ohlcv
  WHERE rsi_14 < 30 AND date >= CURRENT_DATE - INTERVAL 7 DAY
  ORDER BY rsi_14 ASC

Show me AAPL's MACD crossover signals for the last 6 months.
→ SELECT date, close, macd, ema_12, ema_26 FROM ohlcv
  WHERE ticker = 'AAPL' AND date >= CURRENT_DATE - INTERVAL 180 DAY
  ORDER BY date

Which stocks are trading above their 200-day moving average right now?
→ SELECT ticker, date, close, sma_200 FROM ohlcv
  WHERE date = (SELECT MAX(date) FROM ohlcv)
    AND close > sma_200
  ORDER BY ticker

What is QQQ's Bollinger Band width over the last 60 days?
→ SELECT date, close, bb_upper, bb_lower, bb_upper - bb_lower AS band_width
  FROM ohlcv WHERE ticker = 'QQQ'
  ORDER BY date DESC LIMIT 60
```

### Options

```
Show me unusual options volume on AAPL today.
→ SELECT expiration, strike, option_type, volume, open_interest,
         volume / open_interest AS vol_oi_ratio
  FROM options
  WHERE underlying = 'AAPL' AND date = (SELECT MAX(date) FROM options)
    AND volume > open_interest
  ORDER BY volume DESC LIMIT 20

What is the current implied volatility surface for SPY?
→ SELECT expiration, strike, option_type, implied_volatility, mid_price
  FROM options
  WHERE underlying = 'SPY' AND date = (SELECT MAX(date) FROM options)
  ORDER BY expiration, strike

Find the highest open interest SPY puts expiring this month.
→ SELECT expiration, strike, bid, ask, open_interest, implied_volatility
  FROM options
  WHERE underlying = 'SPY' AND option_type = 'put'
    AND expiration <= DATE_TRUNC('month', CURRENT_DATE) + INTERVAL 1 MONTH
  ORDER BY open_interest DESC LIMIT 10

Which options have delta > 0.8 (deep in-the-money calls) on TSLA?
→ SELECT expiration, strike, delta, mid_price, implied_volatility
  FROM options_greeks
  WHERE underlying = 'TSLA' AND option_type = 'call' AND delta > 0.8
    AND date = (SELECT MAX(date) FROM options_greeks)
  ORDER BY expiration, strike

Compare IV rank across SPY, QQQ, and IWM.
→ SELECT underlying,
         AVG(implied_volatility) AS avg_iv,
         MIN(implied_volatility) AS min_iv,
         MAX(implied_volatility) AS max_iv
  FROM options
  WHERE underlying IN ('SPY', 'QQQ', 'IWM')
    AND date >= CURRENT_DATE - INTERVAL 30 DAY
  GROUP BY underlying
  ORDER BY avg_iv DESC
```

### Data exploration

```
What tables are available and how fresh is the data?
→ calls list_tables() — shows all tables with row counts and date ranges

How many days of OHLCV history do I have for NVDA?
→ SELECT COUNT(*) AS trading_days, MIN(date) AS earliest, MAX(date) AS latest
  FROM ohlcv WHERE ticker = 'NVDA'

What is the schema for the options_greeks table?
→ calls describe_table('options_greeks')
```

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `Authentication error` | Check `CF_API_TOKEN` in `.env` — needs R2 read permission |
| `SQL error` | Use `describe_table()` to verify column names before querying |
| `Rate limit hit` | Wait ~30 seconds and retry |
| `account_id is required` | Ensure `CF_ACCOUNT_ID` is set in `.env` or passed via env in the MCP config |
