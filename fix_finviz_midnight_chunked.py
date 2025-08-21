# fix_finviz_midnight_chunked_v2.py
import time
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": "localhost",
    "user": "sentiment_user",
    "password": "eMber0310!",
    "database": "stock_data",
    "autocommit": False
}

CHUNK_SIZE = 2000    # start smaller (1000-2000). Raise if stable.
SLEEP_SECONDS = 0.05

def detect_timestamp_column(cur):
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (DB_CONFIG["database"], "finviz_data"))
    cols = {r[0].lower() for r in cur.fetchall()}
    # prefer scraped_at, then scrape_time, then scrapedtime fallback
    for candidate in ("scraped_at", "scrape_time", "scrape_time_utc", "scrapedtime"):
        if candidate in cols:
            return candidate
    return None

def count_midnight_total(cur):
    cur.execute("SELECT COUNT(*) FROM finviz_data WHERE TIME(inserted_at) = '00:00:00'")
    return cur.fetchone()[0]

def count_midnight_with_col(cur, col):
    q = f"SELECT COUNT(*) FROM finviz_data WHERE TIME(inserted_at) = '00:00:00' AND {col} IS NOT NULL AND inserted_at <> {col}"
    cur.execute(q)
    return cur.fetchone()[0]

def get_ids_to_fix(cur, limit):
    cur.execute(
        "SELECT id FROM finviz_data "
        "WHERE TIME(inserted_at) = '00:00:00' "
        "  AND {col} IS NOT NULL "
        "  AND inserted_at <> {col} "
        "LIMIT %s".format(col=timestamp_col),
        (limit,)
    )
    return [r[0] for r in cur.fetchall()]

def update_by_ids(cur, ids):
    fmt = ",".join(["%s"] * len(ids))
    q = f"UPDATE finviz_data SET inserted_at = {timestamp_col} WHERE id IN ({fmt})"
    cur.execute(q, tuple(ids))
    return cur.rowcount

if __name__ == "__main__":
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Connected to DB.")
        timestamp_col = detect_timestamp_column(cur)
        print("Detected timestamp column for fix:", timestamp_col)

        total_midnight = count_midnight_total(cur)
        print("Rows with midnight inserted_at:", total_midnight)

        if not timestamp_col:
            print("No scraped_at/scrape_time column found. Exiting.")
        else:
            to_fix = count_midnight_with_col(cur, timestamp_col)
            print(f"Rows with midnight inserted_at and non-null {timestamp_col} (and differing):", to_fix)
            fixed_total = 0
            while True:
                # get a chunk of ids to update
                cur.execute(
                    ("SELECT id FROM finviz_data "
                     "WHERE TIME(inserted_at) = '00:00:00' "
                     f"AND {timestamp_col} IS NOT NULL "
                     f"AND inserted_at <> {timestamp_col} "
                     "LIMIT %s"),
                    (CHUNK_SIZE,)
                )
                rows = [r[0] for r in cur.fetchall()]
                if not rows:
                    print("No more rows matching chunk criteria. Done.")
                    break
                # update that chunk
                fmt = ",".join(["%s"] * len(rows))
                q = f"UPDATE finviz_data SET inserted_at = {timestamp_col} WHERE id IN ({fmt})"
                cur.execute(q, tuple(rows))
                conn.commit()
                fixed = cur.rowcount
                fixed_total += fixed
                print(f"Fixed {fixed} rows this chunk (total fixed {fixed_total}). Sleeping {SLEEP_SECONDS}s")
                time.sleep(SLEEP_SECONDS)
            print("Finished. Total rows fixed:", fixed_total)

        cur.close()
        conn.close()

    except Error as e:
        print("MySQL Error:", e)
    except KeyboardInterrupt:
        print("Interrupted by user")