# Duckdbinvestmentdata

A local data pipeline that pulls raw stock price and news data from Yahoo Finance for 9 watchlist companies, stores it in DuckDB, and models it with dbt.

---

## Project Structure

```
Duckdbinvestmentdata/
│
├── fetch_watchlist.py          # Pulls stock prices + news from Yahoo Finance → outputs JSON files
├── duckdb_yahoofinancedata.py  # Loads JSON files into DuckDB raw schema
├── raw_prices.json             # Raw price data (generated, do not edit)
├── raw_news.json               # Raw news data (generated, do not edit)
├── watchlist.duckdb            # Local DuckDB database file
├── dbt_project.yml             # dbt project config
├── models/                     # dbt models (staging, marts)
└── README.md
```

---

## Tickers Tracked

| Ticker | Company               |
|--------|-----------------------|
| OGN    | Organon & Co.         |
| ALAB   | Astera Labs           |
| TTMI   | TTM Technologies      |
| FSLY   | Fastly                |
| AKAM   | Akamai Technologies   |
| SEZL   | Sezzle                |
| NET    | Cloudflare            |
| PLTR   | Palantir Technologies |
| HOLX   | Hologic               |

---

## Stack

- **Python + yfinance** — data extraction from Yahoo Finance
- **DuckDB** — local database for raw data storage
- **dbt (dbt-duckdb)** — data transformation and modeling
- **Power BI** — final visualization and reporting layer

---

## Architecture

```
Yahoo Finance
     │
     ▼
fetch_watchlist.py        ← Python + yfinance
     │
     ▼
raw_prices.json
raw_news.json
     │
     ▼
duckdb_yahoofinancedata.py  ← loads into DuckDB
     │
     ▼
watchlist.duckdb (raw schema)
     │
     ▼
dbt models                  ← staging + marts
     │
     ▼
Power BI Dashboard          ← final visuals
```

---

## How to Run

### 1. Install dependencies
```bash
pip install yfinance duckdb dbt-duckdb
```

### 2. Fetch data from Yahoo Finance
```bash
python fetch_watchlist.py
```
This pulls the last 30 days of price history and latest news for all tickers.
Outputs `raw_prices.json` and `raw_news.json`.

To fetch a custom date range:
```bash
python fetch_watchlist.py --start 2024-01-01 --end 2024-12-31
```

### 3. Load into DuckDB
```bash
python duckdb_yahoofinancedata.py
```
This creates a `raw` schema in `watchlist.duckdb` with two tables:
- `raw.raw_prices` — OHLCV data per ticker per day
- `raw.raw_news` — news articles per ticker

### 4. Run dbt
```bash
dbt debug    # verify connection
dbt run      # run models
dbt test     # run tests
```

---

## DuckDB Schema

### `raw.raw_prices`
| Column        | Description                        |
|---------------|------------------------------------|
| _extracted_at | Timestamp when data was pulled     |
| _source       | Always "yfinance"                  |
| ticker        | Stock ticker symbol                |
| date          | Trading date                       |
| open          | Opening price                      |
| high          | Daily high                         |
| low           | Daily low                          |
| close         | Closing price                      |
| volume        | Trading volume                     |
| dividends     | Dividend amount (if any)           |
| stock_splits  | Stock split ratio (if any)         |

### `raw.raw_news`
| Column            | Description                          |
|-------------------|--------------------------------------|
| _extracted_at     | Timestamp when data was pulled       |
| _source           | Always "yfinance"                    |
| ticker            | Stock ticker symbol                  |
| uuid              | Unique article ID                    |
| title             | Article headline                     |
| publisher         | News source                          |
| link              | Article URL                          |
| published_at_unix | Publish timestamp (Unix)             |
| related_tickers   | Other tickers mentioned              |
| _raw              | Full raw JSON from Yahoo Finance     |

---

## dbt Profile (`~/.dbt/profiles.yml`)

```yaml
Duckdbinvestmentdata:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: C:\Users\geral\OneDrive\Duckdbinvestmentdata\watchlist.duckdb
      schema: raw
```

---

## Notes

- Raw data is intentionally unclean — all transformation happens in dbt models
- Re-run `fetch_watchlist.py` + `duckdb_yahoofinancedata.py` anytime to refresh data
- Yahoo Finance may occasionally rate-limit requests — just re-run if a ticker fails
- The `_raw` column on `raw_news` contains the full unparsed JSON for flexibility in modeling
- Power BI connects to DuckDB via the DuckDB ODBC driver — point it at `watchlist.duckdb` and query the final dbt mart models for visuals
