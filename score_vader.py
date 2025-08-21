import pandas as pd
from sqlalchemy import create_engine, text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

ENGINE     = create_engine("mysql+mysqlconnector://sentiment_user:eMber0310%21@localhost/stock_data")
BATCH_SIZE = 500

analyzer = SentimentIntensityAnalyzer()

def vader_scorer(text: str) -> tuple[float,float]:
    raw = analyzer.polarity_scores(text)["compound"]
    norm = (raw + 1.0) / 2.0
    return raw, norm

def fetch_unscored() -> pd.DataFrame:
    sql = text("""
        SELECT n.id AS news_id, n.headline, n.body
          FROM news n
     LEFT JOIN sentiment_scores s
       ON s.news_id = n.id AND s.engine = 'vader'
         WHERE s.news_id IS NULL
         LIMIT :lim
    """)
    return pd.read_sql(sql, ENGINE, params={"lim": BATCH_SIZE})

def write_scores(df: pd.DataFrame):
    df = df.rename(columns={"raw_score": "raw_score", "norm_score": "normalized"})
    df["engine"]      = "vader"
    df["computed_at"] = pd.Timestamp.utcnow()
    df.to_sql(
        "sentiment_scores",
        ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100
    )

def batch_score():
    while True:
        df = fetch_unscored()
        if df.empty:
            print("✅ No more unscored rows for VADER")
            break

        scored = df["headline"].fillna("") + "\n" + df["body"].fillna("")
        raw_norm = scored.apply(vader_scorer).tolist()
        df["raw_score"], df["norm_score"] = zip(*raw_norm)

        write_scores(df)
        print(f"✅ Scored {len(df)} articles with VADER")

if __name__ == "__main__":
    batch_score()