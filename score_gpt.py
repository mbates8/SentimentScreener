# score_gpt.py
import re
from datetime import datetime
import pandas as pd
from db_utils import make_engine
import openai
import mysql.connector
import os
from urllib.parse import quote_plus
import time

ENGINE = make_engine()
openai.api_key = os.getenv("OPENAI_API_KEY")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "sentiment_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "stock_data"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

BATCH_SIZE = int(os.getenv("GPT_BATCH_SIZE", "50"))
MODEL = os.getenv("GPT_MODEL", "gpt-3.5-turbo")
REQUEST_TIMEOUT = int(os.getenv("GPT_REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("GPT_MAX_RETRIES", "5"))
RETRY_BACKOFF_BASE = float(os.getenv("GPT_RETRY_BASE", "1.0"))

NUM_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)")

def parse_first_number(s: str):
    if not isinstance(s, str):
        return None
    m = NUM_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def interpret_wordy_reply(reply: str):
    if not isinstance(reply, str):
        return None
    r = reply.lower()
    if "very positive" in r or "+1" in r or "1.0" in r:
        return 1.0
    if "very negative" in r or "-1" in r or "-1.0" in r:
        return -1.0
    if "positive" in r:
        return 0.8
    if "negative" in r:
        return -0.8
    if "neutral" in r or "0 (neutral)" in r or "0.0" in r:
        return 0.0
    return None

def gpt_scorer(text: str, max_retries: int = MAX_RETRIES) -> float:
    if not openai.api_key:
        raise RuntimeError("OPENAI_API_KEY not set in environment. See README / set env var.")

    prompt = (
        "Rate the sentiment of this news article from -1 (very negative) to +1 (very positive).\n\n"
        "Return a single numeric score only (e.g. -1, -0.5, 0, 0.8, 1). No extra commentary or explanation.\n\n"
        f"{text}"
    )
    backoff = RETRY_BACKOFF_BASE
    for attempt in range(1, max_retries + 1):
        try:
            resp = openai.ChatCompletion.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                request_timeout=REQUEST_TIMEOUT,
            )
            content = ""
            try:
                content = resp.choices[0].message.content.strip()
            except Exception:
                content = getattr(resp.choices[0], "text", "").strip()

            num = parse_first_number(content)
            if num is None:
                num = interpret_wordy_reply(content)

            if num is None:
                return float("nan")
            if num > 1:
                num = 1.0
            if num < -1:
                num = -1.0
            return float(num)
        except Exception as e:
            err_type = type(e).__name__
            transient = False
            try:
                transient_classes = (
                    openai.error.RateLimitError,
                    openai.error.APIConnectionError,
                    openai.error.APITimeoutError,
                )
                if isinstance(e, transient_classes):
                    transient = True
            except Exception:
                if "RateLimitError" in err_type or "Timeout" in err_type or "APIConnectionError" in err_type:
                    transient = True

            if transient and attempt < max_retries:
                print(f"[gpt] Transient error (attempt {attempt}/{max_retries}): {e}; backing off {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            print(f"[gpt] Non-retryable error or out of retries: {e}")
            return float("nan")
    return float("nan")

def batch_score():
    sql = f"""
        SELECT ticker, headline, body, published_at, url
          FROM news
         WHERE sentiment_gpt IS NULL
         ORDER BY published_at ASC
         LIMIT {int(BATCH_SIZE)}
    """
    df = pd.read_sql(sql, ENGINE)
    if df.empty:
        print("✅ No more unscored rows for GPT")
        return

    df["text"] = df["headline"].fillna("") + "\n\n" + df["body"].fillna("")
    raw_scores = []
    print(f"[gpt] Scoring {len(df)} rows")
    for idx, txt in enumerate(df["text"].tolist()):
        try:
            val = gpt_scorer(txt)
            raw_scores.append(val)
            print(f"[{idx}] score={val}")
        except Exception as e:
            raw_scores.append(float("nan"))
            print(f"ERROR scoring row idx={idx}: {e}")

    df["raw_score"] = raw_scores
    df["normalized"] = df["raw_score"].apply(lambda v: float("nan") if pd.isna(v) else (v + 1.0) / 2.0)

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    update_sql = """
        UPDATE news
           SET sentiment_gpt = %s,
               normalized_gpt = %s,
               gpt_computed_at = %s
         WHERE ticker = %s
           AND published_at = %s
           AND url = %s
    """
    written = 0
    for _, row in df.iterrows():
        try:
            raw_val = None if pd.isna(row["raw_score"]) else float(row["raw_score"])
            norm_val = None if pd.isna(row["normalized"]) else float(row["normalized"])
            cur.execute(update_sql, (
                raw_val,
                norm_val,
                datetime.utcnow(),
                row["ticker"],
                row["published_at"],
                row["url"]
            ))
            written += 1
        except Exception as e:
            print("ERROR updating row:", e)
            continue
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Scored {written} articles with GPT")

if __name__ == "__main__":
    if not openai.api_key:
        print("ERROR: OPENAI_API_KEY not set. Set it in the environment and re-run.")
    else:
        batch_score()