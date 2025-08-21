# aggregate_live.py
import argparse
import time
from datetime import datetime, timedelta, date
import mysql.connector
from typing import List, Optional
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "sentiment_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "stock_data"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

DEFAULT_LOOP_SEC = 15 * 60  # 15 minutes

def ensure_daily_table(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_sentiment (
        ticker VARCHAR(16) NOT NULL,
        date DATE NOT NULL,
        avg_sentiment DOUBLE,
        min_sentiment DOUBLE,
        max_sentiment DOUBLE,
        article_count INT,
        PRIMARY KEY (ticker, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    conn.commit()
    cur.close()

def aggregate_for_date(agg_date: date, engines: Optional[List[str]] = None, engine_mode: str = "vader_only"):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    ensure_daily_table(conn)

    if engine_mode == "vader_only":
        sql = """
            SELECT n.ticker, s.normalized
              FROM sentiment_scores s
              JOIN news n ON n.id = s.news_id
             WHERE DATE(n.published_at) = %s
               AND s.engine = 'vader'
        """
        params = (agg_date,)
    elif engine_mode == "all_engines_avg":
        sql = """
            SELECT n.ticker, AVG(s.normalized) as normalized
              FROM sentiment_scores s
              JOIN news n ON n.id = s.news_id
             WHERE DATE(n.published_at) = %s
             GROUP BY n.ticker
        """
        params = (agg_date,)
    elif engine_mode == "preferred" and engines:
        placeholders = ",".join(["%s"] * len(engines))
        sql = f"""
            SELECT n.ticker, AVG(s.normalized) as normalized
              FROM sentiment_scores s
              JOIN news n ON n.id = s.news_id
             WHERE DATE(n.published_at) = %s
               AND s.engine IN ({placeholders})
             GROUP BY n.ticker
        """
        params = tuple([agg_date] + engines)
    else:
        sql = """
            SELECT n.ticker, s.normalized
              FROM sentiment_scores s
              JOIN news n ON n.id = s.news_id
             WHERE DATE(n.published_at) = %s
               AND s.engine = 'vader'
        """
        params = (agg_date,)

    try:
        cur.execute(sql, params)
    except Exception as e:
        print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] ERROR querying sentiment_scores: {e}")
        cur.close(); conn.close()
        return

    data = {}
    for row in cur.fetchall():
        if len(row) == 2:
            ticker, norm = row
            try:
                norm_f = float(norm) if norm is not None else None
            except Exception:
                norm_f = None
            if norm_f is None:
                continue
            data.setdefault(ticker, []).append(norm_f)

    upsert_sql = """
      INSERT INTO daily_sentiment
        (ticker, date, avg_sentiment, min_sentiment, max_sentiment, article_count)
      VALUES (%s, %s, %s, %s, %s, %s)
      ON DUPLICATE KEY UPDATE
        avg_sentiment = VALUES(avg_sentiment),
        min_sentiment = VALUES(min_sentiment),
        max_sentiment = VALUES(max_sentiment),
        article_count = VALUES(article_count)
    """
    written = 0
    for ticker, scores in data.items():
        avg = sum(scores) / len(scores)
        mn, mx, cnt = min(scores), max(scores), len(scores)
        try:
            cur.execute(upsert_sql, (ticker, agg_date, avg, mn, mx, cnt))
            written += 1
        except Exception as e:
            print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S}] ERROR upserting {ticker}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"[{agg_date}] ✅ Live-aggregated {written} tickers (mode={engine_mode})")

def main():
    parser = argparse.ArgumentParser(description="Live aggregator for daily_sentiment")
    parser.add_argument("--interval", type=int, default=None,
                        help="Run repeatedly every N seconds (default: None)")
    parser.add_argument("--engines", nargs="+", default=None,
                        help="Optional list of engines for 'preferred' mode (e.g. --engines vader finbert gpt)")
    parser.add_argument("--mode", choices=["vader_only", "all_engines_avg", "preferred"], default="vader_only",
                        help="Aggregation mode (default: vader_only)")
    parser.add_argument("--target-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        help="YYYY-MM-DD to aggregate once (optional). If omitted, uses today's date.")
    args = parser.parse_args()

    target = args.target_date or datetime.utcnow().date()

    if args.interval is None:
        aggregate_for_date(target, engines=args.engines, engine_mode=args.mode)
    else:
        try:
            while True:
                aggregate_for_date(datetime.utcnow().date(), engines=args.engines, engine_mode=args.mode)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("aggregate_live stopping via KeyboardInterrupt")

if __name__ == "__main__":
    main()