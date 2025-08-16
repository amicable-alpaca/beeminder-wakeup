#!/usr/bin/env python3
"""
Reconcile Beeminder 'wakeandfocus' with a local Source of Truth (SoT)
computed from Focusmate history.

Logic:
  1) Download Focusmate datapoints (paginated).
  2) For each day in the chosen window, compute SoT outcome:
       1 if there's at least one >=50m Focusmate session that started
       at/before 09:15 local; else 0.
  3) Upsert SoT rows for *all* days in the window.
  4) Reconcile Beeminder 'wakeandfocus' per day:
       - If none: POST (idempotent via requestid) **with daystamp**.
       - If multiple: keep newest, delete extras.
       - Ensure value & comment match SoT; PUT update (with daystamp) if needed.
  5) Optional purge of extra Beeminder days not in SoT (STRICT_PURGE=1).

Env:
  BM_USERNAME   (default: zarathustra)
  BM_AUTH_TOKEN (required unless DRY_RUN=1)
  DRY_RUN=1     (no API mutations)
  DEBUG=1       (extra logs)
  FULL_HISTORY=1  (use earliest Focusmate dp .. today)
  HISTORY_DAYS=N  (default 90; ignored if FULL_HISTORY=1)
  STRICT_PURGE=1  (delete wakeandfocus dps on days not in SoT)
"""

import os
import re
import sys
import sqlite3
import requests
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from collections import defaultdict
from typing import Optional

# -------- Config from env --------
USERNAME = os.getenv("BM_USERNAME", "zarathustra")
AUTH_TOKEN = os.getenv("BM_AUTH_TOKEN")  # REQUIRED unless DRY_RUN=1
DRY_RUN = os.getenv("DRY_RUN") == "1"
DEBUG = os.getenv("DEBUG") == "1"
FULL_HISTORY = os.getenv("FULL_HISTORY") == "1"
STRICT_PURGE = os.getenv("STRICT_PURGE") == "1"
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "90"))

FOCUSMATE_GOAL = "focusmate"
WAKEANDFOCUS_GOAL = "wakeandfocus"

LOCAL_TZ = ZoneInfo("America/New_York")
MIN_SESSION_MINUTES = 50
CUTOFF_HOUR = 9
CUTOFF_MINUTE = 15  # inclusive

COMMENT_RE = re.compile(
    r"^\s*(\d+)\s*minutes?\s+session\s+at\s+(\d{1,2}):(\d{2})\b",
    re.IGNORECASE,
)

API_BASE = "https://www.beeminder.com/api/v1"

# -------- SoT storage --------
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "wake_focus_sot.db"

CREATE_SOT_SQL = """
CREATE TABLE IF NOT EXISTS records (
  daystamp TEXT PRIMARY KEY,
  value INTEGER NOT NULL CHECK(value IN (0,1)),
  updated_at TEXT NOT NULL
);
"""

UPSERT_SOT_SQL = """
INSERT INTO records(daystamp, value, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(daystamp) DO UPDATE SET
  value=excluded.value,
  updated_at=excluded.updated_at;
"""

SELECT_ALL_SOT_SQL = "SELECT daystamp, value FROM records;"

# -------- Helpers --------
def log_debug(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")

def daystamp_of(dt: datetime, tz: ZoneInfo) -> str:
    return dt.astimezone(tz).strftime("%Y%m%d")

def parse_comment_for_length_and_time(comment: Optional[str]):
    m = COMMENT_RE.search(comment or "")
    if not m:
        return None
    minutes = int(m.group(1))
    hour = int(m.group(2))
    minute = int(m.group(3))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return minutes, hour, minute

def qualifies_time(hour: int, minute: int) -> bool:
    return (hour < CUTOFF_HOUR) or (hour == CUTOFF_HOUR and minute <= CUTOFF_MINUTE)

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _dp_updated_key(dp: dict) -> float:
    """Robust sort key for 'newest' dp."""
    u = dp.get("updated_at")
    if isinstance(u, str):
        try:
            # Beeminder tends to use ISO8601 with Z
            return datetime.fromisoformat(u.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    t = dp.get("timestamp")
    if isinstance(t, (int, float)):
        return float(t)
    return 0.0

# -------- API helpers (with pagination) --------
def _need_auth():
    if not AUTH_TOKEN and not DRY_RUN:
        raise RuntimeError("BM_AUTH_TOKEN is not set")

def fetch_all_datapoints(username: str, goal: str) -> list[dict]:
    """Return *all* datapoints for a goal (paginated)."""
    _need_auth()
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would fetch all datapoints for {goal}")
        return []

    results = []
    page = 1
    per_page = 100
    while True:
        url = f"{API_BASE}/users/{username}/goals/{goal}/datapoints.json"
        params = {"auth_token": AUTH_TOKEN, "sort": "desc", "page": page, "per_page": per_page}
        log_debug(f"GET {url} page={page}")
        resp = requests.get(url, params=params, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"GET {goal} page {page} -> {resp.status_code}: {resp.text}") from e
        batch = resp.json()
        if not batch:
            break
        results.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
    log_debug(f"Fetched {len(results)} datapoints for {goal}")
    return results

def add_datapoint(goal: str, value: float, comment: str, *, daystamp: str, requestid: str | None = None):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would POST {goal}: value={value}, comment={comment}, daystamp={daystamp}, requestid={requestid}")
        return {}
    url = f"{API_BASE}/users/{USERNAME}/goals/{goal}/datapoints.json"
    data = {
        "auth_token": AUTH_TOKEN,
        "value": value,
        "comment": comment,
        "daystamp": daystamp,  # YYYYMMDD
    }
    if requestid:
        data["requestid"] = requestid
    resp = requests.post(url, data=data, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"POST {goal} {daystamp} -> {resp.status_code}: {resp.text}") from e
    return resp.json()

def update_datapoint(goal: str, dp_id: str, value: float, comment: str, *, daystamp: Optional[str] = None):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would PUT {goal} dp_id={dp_id}: value={value}, comment={comment}, daystamp={daystamp}")
        return {}
    url = f"{API_BASE}/users/{USERNAME}/goals/{goal}/datapoints/{dp_id}.json"
    data = {"auth_token": AUTH_TOKEN, "value": value, "comment": comment}
    if daystamp:
        data["daystamp"] = daystamp
    resp = requests.put(url, data=data, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"PUT {goal} {dp_id} -> {resp.status_code}: {resp.text}") from e
    return resp.json()

def delete_datapoint(goal: str, dp_id: str):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would DELETE {goal} dp_id={dp_id}")
        return True
    url = f"{API_BASE}/users/{USERNAME}/goals/{goal}/datapoints/{dp_id}.json"
    params = {"auth_token": AUTH_TOKEN}
    resp = requests.delete(url, params=params, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"DELETE {goal} {dp_id} -> {resp.status_code}: {resp.text}") from e
    return True

# -------- SoT compute over a date range --------
def daterange(start: date, end: date):
    """Inclusive range of dates from start to end."""
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)

def compute_sot_for_range(focusmate_dps: list[dict], start_date: date, end_date: date) -> dict[str, int]:
    """
    Return dict[daystamp] -> 0/1 for the requested date range.
    Uses Focusmate comments to decide if the day qualifies.
    """
    # Bucket focusmate dps by daystamp (local)
    by_day: dict[str, list[dict]] = defaultdict(list)
    for dp in focusmate_dps:
        ds = dp.get("daystamp")
        if not ds:
            ts = dp.get("timestamp")
            if ts is None:
                continue
            ds = daystamp_of(datetime.fromtimestamp(ts, tz=timezone.utc), LOCAL_TZ)
        by_day[ds].append(dp)

    out: dict[str, int] = {}
    for d in daterange(start_date, end_date):
        ds = d.strftime("%Y%m%d")
        val = 0
        for dp in by_day.get(ds, []):
            parsed = parse_comment_for_length_and_time(dp.get("comment", ""))
            if not parsed:
                continue
            minutes, hour, minute = parsed
            if minutes >= MIN_SESSION_MINUTES and qualifies_time(hour, minute):
                val = 1
                break
        out[ds] = val
    return out

# -------- SQLite helpers --------
def sot_open():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(CREATE_SOT_SQL)
    conn.commit()
    return conn

def sot_upsert_bulk(conn: sqlite3.Connection, mapping: dict[str, int]):
    now = now_iso_utc()
    with conn:
        for ds, v in mapping.items():
            conn.execute(UPSERT_SOT_SQL, (ds, int(v), now))

def sot_load_all(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.execute(SELECT_ALL_SOT_SQL)
    return {row[0]: int(row[1]) for row in cur.fetchall()}

# -------- Reconciliation across history --------
def reconcile_history(sot_map: dict[str, int]):
    """Bring wakeandfocus dps in line with SoT for all days in sot_map."""
    wf_all = fetch_all_datapoints(USERNAME, WAKEANDFOCUS_GOAL)

    # Group existing wakeandfocus by daystamp
    wf_by_day: dict[str, list[dict]] = defaultdict(list)
    for dp in wf_all:
        ds = dp.get("daystamp")
        if not ds:
            ts = dp.get("timestamp")
            if ts is None:
                continue
            ds = daystamp_of(datetime.fromtimestamp(ts, tz=timezone.utc), LOCAL_TZ)
        wf_by_day[ds].append(dp)

    # 1) For each SoT day: ensure exactly one dp with matching value+comment
    for ds, sot_val in sorted(sot_map.items()):
        comment_ok = f"Auto: SoT={int(sot_val)} for {ds} (≥50m by 09:15 check)."
        existing = sorted(
            wf_by_day.get(ds, []),
            key=_dp_updated_key,
            reverse=True,
        )

        if not existing:
            reqid = f"{WAKEANDFOCUS_GOAL}-{ds}-sot-v1"
            log_debug(f"[{ds}] Missing on Beeminder → POST {int(sot_val)}")
            add_datapoint(WAKEANDFOCUS_GOAL, int(sot_val), comment_ok, daystamp=ds, requestid=reqid)
            continue

        keeper = existing[0]
        extras = existing[1:]
        # delete extras
        for dp in extras:
            dp_id = dp.get("id")
            if dp_id:
                log_debug(f"[{ds}] Deleting duplicate dp {dp_id}")
                delete_datapoint(WAKEANDFOCUS_GOAL, dp_id)

        # ensure keeper matches SoT
        try:
            keeper_value = float(keeper.get("value", 0))
        except Exception:
            keeper_value = 0.0

        needs_value = int(round(keeper_value)) != int(sot_val)
        needs_comment = (keeper.get("comment") or "") != comment_ok

        if needs_value or needs_comment:
            kp_id = keeper.get("id")
            if kp_id:
                log_debug(f"[{ds}] Updating keeper {kp_id} -> {int(sot_val)}")
                update_datapoint(WAKEANDFOCUS_GOAL, kp_id, int(sot_val), comment_ok, daystamp=ds)

    # 2) Optional purge: remove wakeandfocus dps on days not in SoT
    if STRICT_PURGE:
        sot_days = set(sot_map.keys())
        for ds, dps in wf_by_day.items():
            if ds not in sot_days:
                for dp in dps:
                    dp_id = dp.get("id")
                    if dp_id:
                        log_debug(f"[{ds}] STRICT_PURGE delete dp {dp_id}")
                        delete_datapoint(WAKEANDFOCUS_GOAL, dp_id)

# -------- Main --------
def main():
    try:
        # Determine range
        end_date = datetime.now(LOCAL_TZ).date()
        if FULL_HISTORY:
            fm_all = fetch_all_datapoints(USERNAME, FOCUSMATE_GOAL)
            timestamps = [
                dp.get("timestamp")
                for dp in fm_all
                if isinstance(dp.get("timestamp"), (int, float))
            ]
            if timestamps:
                earliest_ts = min(timestamps)
                start_date = datetime.fromtimestamp(earliest_ts, tz=timezone.utc).astimezone(LOCAL_TZ).date()
            else:
                start_date = end_date
            log_debug(f"Range FULL_HISTORY: {start_date} .. {end_date}")
        else:
            start_date = end_date - timedelta(days=HISTORY_DAYS - 1)
            log_debug(f"Range LAST {HISTORY_DAYS} DAYS: {start_date} .. {end_date}")
            fm_all = fetch_all_datapoints(USERNAME, FOCUSMATE_GOAL)

        # Build SoT over the chosen range
        sot_map = compute_sot_for_range(fm_all, start_date, end_date)

        # Upsert SoT to DB
        conn = sot_open()
        sot_upsert_bulk(conn, sot_map)

        # Reconcile across history
        reconcile_history(sot_map)

    except requests.HTTPError as e:
        # Should rarely hit now because we catch/raise with body above
        sys.stderr.write(f"HTTP error: {e} — {e.response.text if e.response else ''}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
