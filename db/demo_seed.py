# db/demo_seed.py
# Safe demo DB seeder for local testing. Uses env vars for credentials - do NOT commit .env with real secrets.

import mysql.connector
import os
from datetime import datetime

DB_CONFIG = {
    "host": os.getenv("DB_HOST","localhost"),
    "user": os.getenv("DB_USER","sentiment_user"),
    "password": os.getenv("DB_PASSWORD",""),
    "database": os.getenv("DB_NAME","stock_data"),
    "port": int(os.getenv("DB_PORT","3306")),
}

schema = """
CREATE TABLE IF NOT EXISTS finviz_data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(16),
  price_change DOUBLE,
  volume BIGINT,
  relative_volume DOUBLE,
  avg_volume BIGINT,
  inserted_at DATETIME
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS news (
  id INT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(16),
  headline TEXT,
  body TEXT,
  published_at DATETIME,
  url TEXT
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS daily_sentiment (
  ticker VARCHAR(16) NOT NULL,
  date DATE NOT NULL,
  avg_sentiment DOUBLE,
  min_sentiment DOUBLE,
  max_sentiment DOUBLE,
  article_count INT,
  PRIMARY KEY (ticker, date)
) CHARACTER SET utf8mb4;
"""

sample_finviz = [
    ("AAPL", 1.2, 1000000, 6.0, 150000, "2025-08-21 12:00:00"),
    ("MSFT", -0.5, 800000, 4.2, 190000, "2025-08-21 12:10:00"),
]

sample_news = [
    ("AAPL", "Apple demo release", "Short body text for demo", "2025-08-21 11:40:00", "https://example.com/1"),
    ("MSFT", "Microsoft quarterly", "Short body text for demo", "2025-08-21 11:50:00", "https://example.com/2"),
]

def main():
    # make a connection using credentials from environment variables
    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"]
    )
    cur = conn.cursor()
    # create database if missing
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4")
    cur.execute(f"USE {DB_CONFIG['database']}")
    # create schema
    for stmt in schema.split(';'):
        s = stmt.strip()
        if s:
            cur.execute(s)
    # insert demo rows
    cur.executemany(
        "INSERT INTO finviz_data (ticker, price_change, volume, relative_volume, avg_volume, inserted_at) VALUES (%s,%s,%s,%s,%s,%s)",
        sample_finviz
    )
    cur.executemany(
        "INSERT INTO news (ticker, headline, body, published_at, url) VALUES (%s,%s,%s,%s,%s)",
        sample_news
    )
    conn.commit()
    cur.close()
    conn.close()
    print("✅ demo DB seeded")

if __name__ == "__main__":
    main()
