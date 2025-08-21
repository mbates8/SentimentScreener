import threading
import subprocess
import schedule
import time
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List
import requests
import re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import os
from urllib.parse import quote_plus

# Use same python interpreter that launched this process
PYTHON = subprocess.sys.executable

# ---- Config ----
DEMO_MAX_SYMBOLS = 200
PAUSE_BETWEEN_PAGES = 0.9     # seconds between finviz page fetches
TICKERS_PATH = "tickers_news.txt"
FINVIZ_PAGE_SIZE = 20         # finviz page step size (r=1,21,41...)
# missing-date helper controls
MISSING_LOOKBACK_DAYS = 180        # days back to scan if MISSING_START_DATE is None

MISSING_START_DATE: date | None = date(2025, 6, 10)
# DB
DB_URL = os.getenv("DATABASE_URL") or (
    f"mysql+mysqlconnector://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}/{DB_NAME}"
)
# status file used by web UI for polling
STATUS_PATH = Path("status.json")
# ----------------------------------

engine = create_engine(DB_URL, pool_pre_ping=True)


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

        import re as _re  # local alias so top-level re stays unchanged
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.find_all("a", href=_re.compile(r"quote\.ashx\?t="))
        page_added = 0

        for a in anchors:
            href = a.get("href", "")
            m = _re.search(r"quote\.ashx\?t=([A-Z0-9\.-]+)", href, _re.I)
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

        if page_added == 0:
            break
        if max_symbols and len(tickers) >= max_symbols:
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


# status helpers
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
    """Blocking run (used for bootstrapping or synchronous backfills)."""
    cmd = [PYTHON, script] + (args or [])
    log(f"Running (sync) {cmd}")
    try:
        subprocess.run(cmd, check=check)
    except subprocess.CalledProcessError as e:
        log(f"ERROR running {script}: returncode={e.returncode}")
    except Exception as e:
        log(f"EXC running {script}: {e}")


def run_script_locked(script: str, args: list | None = None):
    """
    Run script in a lock so that multiple scheduled invocations don't overlap.
    Lock key includes args now, so different arg variants (e.g. --live-only vs --target-date)
    can run concurrently.
    """
    args = args or []
    lock_key = script + " " + " ".join(map(str, args))
    lock = _get_lock_for_key(lock_key)
    if not lock.acquire(blocking=False):
        log(f"Skips {script} {args} because previous run still active")
        return

    # mark active
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
        # unmark active
        with _active_scripts_lock:
            if lock_key in _active_scripts:
                _active_scripts.remove(lock_key)
        write_status()
        lock.release()


def schedule_call(script: str, args: list | None = None):
    threading.Thread(target=run_script_locked, args=(script, args), daemon=True).start()


# ---------------- missing-date backfill (one-shot) ----------------
def find_missing_dates(start_date: date, end_date: date) -> List[date]:
    """Return list of dates between start_date..end_date (inclusive) that do NOT have rows in finviz_data."""
    try:
        with engine.connect() as conn:
            # fetch existing dates in the range
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
    """
    One-shot: scan for missing dates and run historical backfills sequentially.
    This blocks until done (or errors). After returning, orchestrator should start live mode.
    """
    today = datetime.utcnow().date()
    if MISSING_START_DATE is not None:
        earliest = MISSING_START_DATE
    else:
        earliest = today - timedelta(days=lookback_days)

    # only backfill up to yesterday (do not try to backfill today in historical mode)
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
            # Run the historical backfill synchronously so we don't intermix live runs
            run_script_sync("backfill_news_real.py", args=["--target-date", d.isoformat()], check=False)
            # small pause between days to be gentle on resources
            time.sleep(1)
        except Exception as e:
            log(f"[backfill-once] Error backfilling {d}: {e}")
    log("[backfill-once] Historical backfill pass complete.")


# ---------------- universe refresh ----------------
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


# ---------------- main bootstrap & scheduler ----------------
if __name__ == "__main__":
    # 1) initial universe fetch (blocking)
    refresh_universe(max_symbols=DEMO_MAX_SYMBOLS)

    # 1b) ensure status.json exists immediately
    write_status()

    # 2) Do a one-shot historical missing-date backfill (blocking) BEFORE starting live loop
    # This ensures historical catch-up without live interference.
    try:
        missing_date_backfill_once(lookback_days=MISSING_LOOKBACK_DAYS)
    except Exception as e:
        log(f"Exception during one-shot missing-date backfill: {e}")

    # 3) Now start the live continuous backfill loop (background) and helper scheduling
    threading.Thread(
        target=lambda: run_script_locked("backfill_news_real.py", args=["--live-only"]),
        daemon=True,
    ).start()
    log("Started continuous backfill loop (background thread) [--live-only]")

    # 4) Bootstrapping
    for script in [
        "finviz_auto.py",
        # historical backfill intentionally removed from bootstrap to avoid duplicate runs
        "score_vader.py",
        "score_finbert.py",
    ]:
        log(f"Bootstrapping {script}")
        run_script_sync(script, check=True)

    # Start continuous live aggregator in background (updates daily_sentiment repeatedly)
    log("Starting continuous live aggregator (aggregate_live.py) in background")
    threading.Thread(
        target=lambda: run_script_locked(
            "aggregate_live.py",
            args=["--interval", "300", "--mode", "vader_only"]
        ),
        daemon=True,
    ).start()

    # Keep nightly historical aggregate for completeness
    schedule.every().day.at("00:05").do(lambda: schedule_call("aggregate_daily.py", args=[]))
    log("Scheduled nightly aggregate_daily.py at 00:05")

    # 5) schedule tasks (live)
    schedule.every().hour.do(lambda: threading.Thread(target=refresh_universe, daemon=True).start())

    # frequent small jobs
    schedule.every(5).seconds.do(lambda: schedule_call("finviz_auto.py"))
    schedule.every(5).seconds.do(lambda: schedule_call("backfill_news_real.py", args=["--live-only"]))
    schedule.every(5).seconds.do(lambda: schedule_call("score_vader.py"))
    schedule.every(5).seconds.do(lambda: schedule_call("score_finbert.py"))

    # GPT heavy: hourly
    schedule.every().hour.at(":00").do(lambda: schedule_call("score_gpt.py"))

    log("Live scheduler running (backfills are live-only from now on)")

    # write status now (some scripts may start via schedule)
    write_status()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        log("Orchestrator stopping via KeyboardInterrupt")
