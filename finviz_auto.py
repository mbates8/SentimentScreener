# finviz_auto.py
import csv
import io
import random
import time
from datetime import datetime, date
import requests
import mysql.connector
import os
from urllib.parse import quote_plus

# ---------- user config ----------
ASPXAUTH = os.getenv("FINVIZ_ASPXAUTH", "")

EXPORT_URL = (
    "https://elite.finviz.com/export.ashx"
    "?v=151"
    "&f=sh_avgvol_o500,sh_price_o5,sh_relvol_o1"
    "&o=ticker"
)

COOKIE_HEADER = (
    f".ASPXAUTH={ASPXAUTH}; "
    "screenerUrl=v%3D151"
      "%26f%3Dsh_avgvol_o500%2Csh_price_o5%2Csh_relvol_o1"
      "%26o%3Dticker"
      "%26c%3D1%2C9%2C10%2C11%2C12%2C65%2C66%2C67; "
    "screenerCustomTable=0%2C1%2C2%2C63%2C64%2C67%2C66"
)

HEADERS = {
    "User-Agent":      "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://elite.finviz.com/screener.ashx",
    "Cookie":          COOKIE_HEADER
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "sentiment_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "stock_data"),
    "port": int(os.getenv("DB_PORT", "3306")),
}
# -------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)

def _parse_num_with_suffix(s):
    if s is None:
        return 0
    s = str(s).strip().replace(",", "")
    if s == "" or s == "-" or s.upper() == "N/A":
        return 0
    try:
        last = s[-1].upper()
        if last == "K":
            return float(s[:-1]) * 1_000
        if last == "M":
            return float(s[:-1]) * 1_000_000
        if last == "B":
            return float(s[:-1]) * 1_000_000_000
        if "." in s:
            return float(s)
        return int(float(s))
    except Exception:
        try:
            return float(s)
        except Exception:
            return 0

def safe_fetch_csv(max_tries=6):
    wait = 1.0
    for attempt in range(max_tries):
        try:
            r = session.get(EXPORT_URL, timeout=20)
            r.raise_for_status()
            return r.text
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 429:
                backoff = wait + random.random()
                print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] 429 from finviz, backing off {backoff:.1f}s")
                time.sleep(backoff)
                wait *= 2
                continue
            print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] HTTP error fetching CSV: {e}")
            return ""
        except Exception as e:
            print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Network error fetching CSV: {e}")
            time.sleep(wait)
            wait *= 2
    print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Giving up after {max_tries} tries")
    return ""

def ensure_universe_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickers_universe (
            ticker VARCHAR(16) NOT NULL PRIMARY KEY,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    conn.commit()
    cur.close()

def ensure_date_tag_column(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'finviz_data' AND COLUMN_NAME = 'date_tag'
    """, (DB_CONFIG["database"],))
    exists = cur.fetchone()[0]
    if exists == 0:
        print("Adding date_tag column to finviz_data...")
        cur.execute("ALTER TABLE finviz_data ADD COLUMN date_tag DATE DEFAULT NULL")
        conn.commit()
    cur.close()

def fetch_and_store_finviz(target_date: date):
    tag_date = target_date or datetime.utcnow().date()
    tag_date_str = datetime.combine(tag_date, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
    scraped_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    csv_text = safe_fetch_csv()
    if not csv_text:
        print("No CSV content fetched, exiting.")
        return

    reader = csv.DictReader(io.StringIO(csv_text))

    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        ensure_date_tag_column(conn)
        cur = conn.cursor()

        inserted_count = 0
        tickers_seen = []

        for row in reader:
            try:
                ticker = (row.get("Ticker") or row.get("ticker") or "").strip()
                if not ticker:
                    continue

                change_raw = (row.get("Change") or "").strip().replace("%", "")
                price_change = float(change_raw) if change_raw not in ("", "-", None, "") else 0.0

                vol_raw = row.get("Volume") or row.get("Vol") or ""
                volume = int(_parse_num_with_suffix(vol_raw))

                rel_raw = row.get("Relative Volume") or row.get("Rel Volume") or ""
                relative_volume = float(_parse_num_with_suffix(rel_raw))

                avg_raw = row.get("Average Volume") or row.get("Avg Volume") or ""
                avg_volume = int(_parse_num_with_suffix(avg_raw))

                cur.execute(
                    """
                    INSERT INTO finviz_data
                      (ticker, price_change, volume, relative_volume, avg_volume, inserted_at, scraped_at, date_tag)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      price_change    = VALUES(price_change),
                      volume          = VALUES(volume),
                      relative_volume = VALUES(relative_volume),
                      avg_volume      = VALUES(avg_volume),
                      inserted_at     = VALUES(inserted_at),
                      scraped_at      = VALUES(scraped_at),
                      date_tag        = VALUES(date_tag)
                    """,
                    (
                        ticker.upper(),
                        price_change,
                        volume,
                        relative_volume,
                        avg_volume,
                        scraped_str,
                        scraped_str,
                        tag_date_str
                    )
                )
                tickers_seen.append(ticker.upper())
                inserted_count += 1

            except Exception as e:
                print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Skipping row for {row.get('Ticker')} due to: {e}")
                continue

        conn.commit()
        print(f"[{tag_date}] ✅ Inserted/updated {inserted_count} rows into finviz_data")

        ensure_universe_table(conn)
        if tickers_seen:
            now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            try:
                cur.execute("DELETE FROM tickers_universe")
                vals = [(t, now_str) for t in tickers_seen]
                cur.executemany("INSERT INTO tickers_universe (ticker, updated_at) VALUES (%s,%s)", vals)
                conn.commit()
                print(f"Stored {len(tickers_seen)} tickers in tickers_universe")
            except Exception as e:
                print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Error writing tickers_universe: {e}")
                conn.rollback()

    except Exception as e:
        print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] Fatal error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch & store Finviz CSV data")
    parser.add_argument(
        "--target-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="YYYY-MM-DD to tag rows (default = today)",
    )
    args = parser.parse_args()
    tgt = args.target_date or datetime.utcnow().date()
    fetch_and_store_finviz(tgt)