# aggregate_daily.py
import argparse
import mysql.connector
from datetime import datetime, timedelta, date
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "sentiment_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "stock_data"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

def aggregate_for_date(agg_date: date):
    """Compute daily_sentiment for 'agg_date' using VADER scores."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
      SELECT n.ticker, s.normalized
        FROM sentiment_scores s
        JOIN news n 
          ON n.id = s.news_id
       WHERE DATE(n.published_at) = %s
         AND s.engine = 'vader'
    """, (agg_date,))
    
    data = {}
    for ticker, norm in cur:
        data.setdefault(ticker, []).append(norm)
    
    for ticker, scores in data.items():
        avg = sum(scores) / len(scores)
        mn, mx, cnt = min(scores), max(scores), len(scores)
        cur.execute("""
          INSERT INTO daily_sentiment
            (ticker, date, avg_sentiment, min_sentiment, max_sentiment, article_count)
          VALUES (%s, %s, %s, %s, %s, %s)
          ON DUPLICATE KEY UPDATE
            avg_sentiment = VALUES(avg_sentiment),
            min_sentiment = VALUES(min_sentiment),
            max_sentiment = VALUES(max_sentiment),
            article_count = VALUES(article_count)
        """, (ticker, agg_date, avg, mn, mx, cnt))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"[{agg_date}] ✅ Aggregated {len(data)} tickers")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                        help="YYYY-MM-DD to aggregate (default = yesterday UTC)")
    args = parser.parse_args()
    
    if args.target_date:
        dates = [args.target_date]
    else:
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()
        dates = [yesterday]

    for dt in dates:
        aggregate_for_date(dt)
    
if __name__ == "__main__":
    main()