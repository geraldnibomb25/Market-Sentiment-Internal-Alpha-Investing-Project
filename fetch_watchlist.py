"""
Yahoo Finance Watchlist Scraper
--------------------------------
Pulls raw price history + news for your 14 watchlist tickers.
Outputs two messy JSON files ready for dbt:
  - raw_prices.json
  - raw_news.json

Usage:
  pip install yfinance
  python fetch_watchlist.py

Optionally pass a date range:
  python fetch_watchlist.py --start 2024-01-01 --end 2024-12-31
"""

import yfinance as yf
import json
import argparse
from datetime import datetime, timedelta

# ── YOUR 14 WATCHLIST TICKERS ─────────────────────────────────────────────────
# Update this list with your remaining 5 tickers if different
TICKERS = [
    "OGN",   # Organon & Co.
    "ALAB",  # Astera Labs
    "TTMI",  # TTM Technologies
    "FSLY",  # Fastly
    "AKAM",  # Akamai Technologies
    "SEZL",  # Sezzle
    "NET",   # Cloudflare
    "PLTR",  # Palantir Technologies
    "HOLX",  # Hologic
    # ── Add your remaining 5 tickers below ──
    # "XXXX",
    # "XXXX",
    # "XXXX",
    # "XXXX",
    # "XXXX",
]

# ── ARGS ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--start", default=(datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d"))
parser.add_argument("--end",   default=datetime.today().strftime("%Y-%m-%d"))
args = parser.parse_args()

print(f"\nFetching data from {args.start} → {args.end}")
print(f"Tickers: {TICKERS}\n")

# ── FETCH ─────────────────────────────────────────────────────────────────────
raw_prices = []
raw_news   = []

for symbol in TICKERS:
    print(f"  Pulling {symbol}...")
    ticker = yf.Ticker(symbol)

    # ── PRICE HISTORY (one row per trading day) ────────────────────────────
    try:
        hist = ticker.history(start=args.start, end=args.end)
        hist.index = hist.index.astype(str)  # convert Timestamps to strings

        for date, row in hist.iterrows():
            raw_prices.append({
                # metadata
                "_extracted_at": datetime.utcnow().isoformat(),
                "_source":        "yfinance",

                # identifiers
                "ticker":         symbol,
                "date":           date,

                # raw OHLCV — intentionally unclean (nulls, floats, etc.)
                "open":           row.get("Open"),
                "high":           row.get("High"),
                "low":            row.get("Low"),
                "close":          row.get("Close"),
                "volume":         row.get("Volume"),
                "dividends":      row.get("Dividends"),
                "stock_splits":   row.get("Stock Splits"),
            })
    except Exception as e:
        print(f"    WARNING: price fetch failed for {symbol}: {e}")
        raw_prices.append({
            "_extracted_at": datetime.utcnow().isoformat(),
            "_source":       "yfinance",
            "ticker":        symbol,
            "_error":        str(e),
        })

    # ── NEWS (raw, unstructured — as messy as Yahoo gives it) ─────────────
    try:
        news_items = ticker.news or []
        for item in news_items:
            raw_news.append({
                # metadata
                "_extracted_at":  datetime.utcnow().isoformat(),
                "_source":         "yfinance",

                # which ticker triggered this pull
                "ticker":          symbol,

                # everything Yahoo returns — left raw intentionally
                "uuid":            item.get("id"),
                "title":           item.get("title"),
                "publisher":       item.get("source", {}).get("homepageUrl") if isinstance(item.get("source"), dict) else item.get("publisher"),
                "link":            item.get("canonicalUrl", {}).get("url") if isinstance(item.get("canonicalUrl"), dict) else item.get("link"),
                "published_at_unix": item.get("pubDate") or item.get("providerPublishTime"),
                "type":            item.get("contentType") or item.get("type"),
                "thumbnail_url":   (
                    item.get("thumbnail", {}).get("resolutions", [{}])[0].get("url")
                    if isinstance(item.get("thumbnail"), dict) else None
                ),
                "related_tickers": item.get("relatedTickers", []),

                # dump the entire raw item so dbt can parse anything we missed
                "_raw":            item,
            })
    except Exception as e:
        print(f"    WARNING: news fetch failed for {symbol}: {e}")
        raw_news.append({
            "_extracted_at": datetime.utcnow().isoformat(),
            "_source":       "yfinance",
            "ticker":        symbol,
            "_error":        str(e),
        })

# ── WRITE JSON ────────────────────────────────────────────────────────────────
with open("raw_prices.json", "w") as f:
    json.dump(raw_prices, f, indent=2, default=str)

with open("raw_news.json", "w") as f:
    json.dump(raw_news, f, indent=2, default=str)

print(f"\nDone.")
print(f"  raw_prices.json → {len(raw_prices)} rows")
print(f"  raw_news.json   → {len(raw_news)} articles")
print(f"\nLoad these into your dbt seeds/ or sources/ folder.")
