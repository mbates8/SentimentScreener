# tickers_real.py
import time
import requests
from datetime import datetime
import os
from urllib.parse import quote_plus

TICKERS_FILE = "tickers.txt"
OUTPUT_FILE  = "tickers_news.txt"

from finviz_auto import HEADERS

session = requests.Session()
session.headers.update(HEADERS)

def load_tickers(path=TICKERS_FILE):
    with open(path) as f:
        return [line.strip().upper() for line in f if line.strip()]

def filter_news_covered(tickers):
    valid = []
    for sym in tickers:
        url = f"https://elite.finviz.com/quote.ashx?t={sym}"
        try:
            r = session.head(url, timeout=5)
            if r.status_code == 200:
                valid.append(sym)
            else:
                print(f"[{datetime.now()}] Skipping {sym}: {r.status_code}")
        except Exception as e:
            print(f"[{datetime.now()}] Error on {sym}: {e}")
        time.sleep(0.1)
    return valid

if __name__ == "__main__":
    tickers = load_tickers()
    good     = filter_news_covered(tickers)
    with open(OUTPUT_FILE, "w") as f:
        for sym in good:
            f.write(sym + "\n")
    print(f"Wrote {len(good)} tickers to {OUTPUT_FILE}")