# score_finbert.py
import pandas as pd
from sqlalchemy import text
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
import os
from db_utils import make_engine

# Use SQLAlchemy engine for reads/writes via pandas
ENGINE = make_engine()
BATCH_SIZE = int(os.getenv("FINBERT_BATCH_SIZE", "200"))
MODEL_NAME = os.getenv("FINBERT_MODEL", "yiyanghkust/finbert-tone")

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model     = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

def finbert_scorer(text: str):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs= model(**inputs)
    probs   = F.softmax(outputs.logits, dim=-1).detach().numpy()[0]
    raw     = float(probs[2] - probs[0])
    norm    = (raw + 1.0) / 2.0
    return raw, norm

def fetch_unscored() -> pd.DataFrame:
    sql = text("""
        SELECT n.id AS news_id, n.headline, n.body
          FROM news n
     LEFT JOIN sentiment_scores s
       ON s.news_id = n.id AND s.engine = 'finbert'
         WHERE s.news_id IS NULL
         LIMIT :lim
    """)
    return pd.read_sql(sql, ENGINE, params={"lim": BATCH_SIZE})

def write_scores(df: pd.DataFrame):
    df = df.rename(columns={"raw_score": "raw_score", "norm_score": "normalized"})
    df["engine"]      = "finbert"
    df["computed_at"] = pd.Timestamp.utcnow()
    df.to_sql(
        "sentiment_scores",
        ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=50
    )

def batch_score():
    while True:
        df = fetch_unscored()
        if df.empty:
            print("✅ No more unscored rows for FinBERT")
            break

        scored = df["headline"].fillna("") + "\n" + df["body"].fillna("")
        raw_norm = scored.apply(finbert_scorer).tolist()
        df["raw_score"], df["norm_score"] = zip(*raw_norm)

        write_scores(df)
        print(f"✅ Scored {len(df)} articles with FinBERT")

if __name__ == "__main__":
    batch_score()