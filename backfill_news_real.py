# backfill_news_real.py
import threading
import subprocess
import schedule
import time
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Optional
import requests
import re
from bs4 import BeautifulSoup
from sqlalchemy import text
import os
from db_utils import make_engine

PYTHON = subprocess.sys.executable

# ---- Config ----
DEMO_MAX_SYMBOLS = int(os.getenv("DEMO_MAX_SYMBOLS", "200"))
PAUSE_BETWEEN_PAGES = float(os.getenv("PAUSE_BETWEEN_PAGES", "0.9"))
TICKERS_PATH = "tickers_news.txt"
FINVIZ_PAGE_SIZE = 20
MISSING_LOOKBACK_DAYS = int(os.getenv("MISSING_LOOKBACK_DAYS", "180"))
from typing import Optional as _Optional
MISSING_START_DATE: _Optional[date] = None

# DB engine via SQLAlchemy for status queries
engine = make_engine()

STATUS_PATH = Path("status.json")

def log(msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)

# ---------------- Finviz loader ----------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def load_tickers_from_finviz(max_symbols: int | None = None, pause: float = 0.9) -> List[str]:
    session = requests.Session()
    session.headers.update(HEADERS)
    tickers: List[str] = []
    seen = set()
    offset = 1

    while True:
        url = f"https://finviz.com/screener.ashx?v=111&r={offset}"
        try:
            r = session.get(url, timeout=12)
            r.raise_for_status()
        except Exception as e:
            log(f"[finviz] fetch error offset={offset}: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.find_all("a", href=re.compile(r"quote\.ashx\?t="))
        page_added = 0

        for a in anchors:
            href = a.get("href", "")
            m = re.search(r"quote\.ashx\?t=([A-Z0-9\.-]+)", href, re.I)
            if not m:
                continue
            symbol = m.group(1).upper().strip()
            if symbol in seen:
                continue
            seen.add(symbol)
            tickers.append(symbol)
            page_added += 1
            if max_symbols and len(tickers) >= max_symbols:
                break

        if page_added == 0 or (max_symbols and len(tickers) >= max_symbols):
            break

        offset += FINVIZ_PAGE_SIZE
        time.sleep(pause)

    return tickers

def write_tickers_file(tickers: List[str], path: str = TICKERS_PATH):
    with open(path, "w", encoding="utf-8") as f:
        for t in tickers:
            f.write(f"{t}\n")
    log(f"Wrote {len(tickers)} tickers to {path}")

# ---------------- subprocess runner & locking ----------------
_script_locks: dict = {}
_locks_lock = threading.Lock()

def _get_lock_for_key(key: str):
    with _locks_lock:
        if key not in _script_locks:
            _script_locks[key] = threading.Lock()
        return _script_locks[key]

_active_scripts = set()
_active_scripts_lock = threading.Lock()

def _get_last_finviz_update():
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT MAX(inserted_at) FROM finviz_data"))
            val = r.scalar() if hasattr(r, "scalar") else (r.fetchone()[0] if r.rowcount != 0 else None)
            if val is None:
                return None
            try:
                return val.isoformat(sep=" ", timespec="seconds")
            except Exception:
                return str(val)
    except Exception:
        return None

def write_status():
    with _active_scripts_lock:
        active = sorted(list(_active_scripts))

    payload = {
        "running": bool(active),
        "active_scripts": active,
        "last_finviz_update": _get_last_finviz_update(),
        "status_written_at": datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
    }
    try:
        tmp = STATUS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(STATUS_PATH)
    except Exception as e:
        log(f"Failed to write status.json: {e}")

def run_script_sync(script: str, args: list | None = None, check: bool = False):
    cmd = [PYTHON, script] + (args or [])
    log(f"Running (sync) {cmd}")
    try:
        subprocess.run(cmd, check=check)
    except subprocess.CalledProcessError as e:
        log(f"ERROR running {script}: returncode={e.returncode}")
    except Exception as e:
        log(f"EXC running {script}: {e}")

def run_script_locked(script: str, args: list | None = None):
    args = args or []
    lock_key = script + " " + " ".join(map(str, args))
    lock = _get_lock_for_key(lock_key)
    if not lock.acquire(blocking=False):
        log(f"Skips {script} {args} because previous run still active")
        return

    try:
        with _active_scripts_lock:
            _active_scripts.add(lock_key)
        write_status()

        cmd = [PYTHON, script] + args
        log(f"Starting {cmd}")
        try:
            subprocess.run(cmd)
            log(f"Finished {script} {args}")
        except Exception as e:
            log(f"ERROR in {script} {args}: {e}")
    finally:
        with _active_scripts_lock:
            if lock_key in _active_scripts:
                _active_scripts.remove(lock_key)
        write_status()
        lock.release()

def schedule_call(script: str, args: list | None = None):
    threading.Thread(target=run_script_locked, args=(script, args), daemon=True).start()

# ---------------- missing-date backfill (one-shot) ----------------
def find_missing_dates(start_date: date, end_date: date) -> List[date]:
    try:
        with engine.connect() as conn:
            q = text(
                """
                SELECT DATE(inserted_at) AS d, COUNT(*) AS cnt
                  FROM finviz_data
                 WHERE DATE(inserted_at) BETWEEN :start AND :end
                 GROUP BY DATE(inserted_at)
                """
            )
            res = conn.execute(q, {"start": start_date.isoformat(), "end": end_date.isoformat()})
            existing = {row[0] for row in res.fetchall() if row[0] is not None}
    except Exception as e:
        log(f"[missing] DB error while scanning existing dates: {e}")
        existing = set()

    missing = []
    d = start_date
    while d <= end_date:
        if d not in existing:
            missing.append(d)
        d += timedelta(days=1)
    return missing

def missing_date_backfill_once(lookback_days: int = MISSING_LOOKBACK_DAYS):
    today = datetime.utcnow().date()
    earliest = MISSING_START_DATE or (today - timedelta(days=lookback_days))
    last_day = today - timedelta(days=1)
    if earliest > last_day:
        log("[backfill-once] no range to check (earliest > yesterday)")
        return

    log(f"[backfill-once] scanning for missing dates between {earliest} and {last_day}")
    missing = find_missing_dates(earliest, last_day)
    if not missing:
        log("[backfill-once] No missing historical dates found (caught up).")
        return

    log(f"[backfill-once] Found {len(missing)} missing date(s). Running backfills sequentially...")
    for d in missing:
        try:
            log(f"[backfill-once] Backfilling {d}")
            run_script_sync("backfill_news_real.py", args=["--target-date", d.isoformat()], check=False)
            time.sleep(1)
        except Exception as e:
            log(f"[backfill-once] Error backfilling {d}: {e}")
    log("[backfill-once] Historical backfill pass complete.")

def refresh_universe(max_symbols: int | None = DEMO_MAX_SYMBOLS):
    try:
        log("Refreshing Finviz universe...")
        tickers = load_tickers_from_finviz(max_symbols=max_symbols, pause=PAUSE_BETWEEN_PAGES)
        if tickers:
            write_tickers_file(tickers, TICKERS_PATH)
            log(f"Universe refresh: {len(tickers)} symbols (max={max_symbols})")
        else:
            log("Universe refresh returned no tickers; leaving previous file intact (if any).")
    except Exception as e:
        log(f"Exception while refreshing universe: {e}")

if __name__ == "__main__":
    refresh_universe(max_symbols=DEMO_MAX_SYMBOLS)
    write_status()
    try:
        missing_date_backfill_once(lookback_days=MISSING_LOOKBACK_DAYS)
    except Exception as e:
        log(f"Exception during one-shot missing-date backfill: {e}")

    threading.Thread(
        target=lambda: run_script_locked("backfill_news_real.py", args=["--live-only"]),
        daemon=True,
    ).start()
    log("Started continuous backfill loop (background thread) [--live-only]")

    for script in ["finviz_auto.py", "score_vader.py", "score_finbert.py"]:
        log(f"Bootstrapping {script}")
        run_script_sync(script, check=True)

    threading.Thread(
        target=lambda: run_script_locked("aggregate_live.py", args=["--interval", "300", "--mode", "vader_only"]),
        daemon=True,
    ).start()
    log("Starting continuous live aggregator (aggregate_live.py) in background")

    schedule.every().day.at("00:05").do(lambda: schedule_call("aggregate_daily.py", args=[]))
    schedule.every().hour.do(lambda: threading.Thread(target=refresh_universe, daemon=True).start())

    schedule.every(5).seconds.do(lambda: schedule_call("finviz_auto.py"))
    schedule.every(5).seconds.do(lambda: schedule_call("backfill_news_real.py", args=["--live-only"]))
    schedule.every(5).seconds.do(lambda: schedule_call("score_vader.py"))
    schedule.every(5).seconds.do(lambda: schedule_call("score_finbert.py"))
    schedule.every().hour.at(":00").do(lambda: schedule_call("score_gpt.py"))

    log("Live scheduler running (backfills are live-only from now on)")
    write_status()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        log("Orchestrator stopping via KeyboardInterrupt")
