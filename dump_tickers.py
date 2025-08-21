# dump_tickers.py
import mysql.connector
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "sentiment_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "stock_data"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

def dump_tickers(path="tickers.txt"):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("SELECT DISTINCT ticker FROM finviz_data ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    with open(path, "w") as f:
        for t in tickers:
            f.write(t + "\n")
    print(f"Wrote {len(tickers)} tickers to {path}")

if __name__=="__main__":
    dump_tickers()