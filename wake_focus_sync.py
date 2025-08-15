#!/usr/bin/env python3
"""
Checks today's Focusmate datapoints on Beeminder and:
- Computes today's wake-and-focus outcome (1 if there's at least one >=50-minute
  Focusmate session that started at or before 09:15 local; else 0).
- Writes the result to a local SQLite "source of truth" (SoT).
- RECONCILES Beeminder goal 'wakeandfocus' to match the SoT:
    * Ensures today's datapoint equals the SoT value.
    * Removes any duplicates for today if present.
- Only after reconciliation does it post/update Beeminder as needed.

Requirements:
  pip install requests

Environment variables:
  BM_USERNAME (default: zarathustra)
  BM_AUTH_TOKEN (required)
  DRY_RUN=1 to skip API writes
  DEBUG=1   to print extra info
"""

import os
import re
import sys
import sqlite3
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

# --- Config from env ---
USERNAME = os.getenv("BM_USERNAME", "zarathustra")
AUTH_TOKEN = os.getenv("BM_AUTH_TOKEN")  # REQUIRED unless DRY_RUN=1
DRY_RUN = os.getenv("DRY_RUN") == "1"
DEBUG = os.getenv("DEBUG") == "1"

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

# --- SoT storage ---
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

SELECT_SOT_SQL = "SELECT value FROM records WHERE daystamp=?;"

# --- Helpers ---
def log_debug(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

def today_daystamp(tz: ZoneInfo) -> str:
    return datetime.now(tz).strftime("%Y%m%d")

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_datapoints_for_goal(username: str, goal: str, auth_token: str):
    if not auth_token and not DRY_RUN:
        raise RuntimeError("BM_AUTH_TOKEN is not set")
    url = f"{API_BASE}/users/{username}/goals/{goal}/datapoints.json"
    params = {"auth_token": auth_token, "per_page": 100, "sort": "desc"}
    log_debug(f"GET {url} params={params}")
    if DRY_RUN:
        return []
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()

def add_datapoint(username: str, goal: str, auth_token: str, value: float, comment: str = "", requestid: str | None = None):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would POST {goal}: value={value}, comment={comment}")
        return {}
    url = f"{API_BASE}/users/{username}/goals/{goal}/datapoints.json"
    data = {"auth_token": auth_token, "value": value, "comment": comment}
    if requestid:
        data["requestid"] = requestid
    log_debug(f"POST {url} data={data}")
    resp = requests.post(url, data=data, timeout=20)
    resp.raise_for_status()
    return resp.json()

def update_datapoint(username: str, goal: str, auth_token: str, dp_id: str, value: float, comment: str = ""):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would PUT {goal} dp_id={dp_id}: value={value}, comment={comment}")
        return {}
    url = f"{API_BASE}/users/{username}/goals/{goal}/datapoints/{dp_id}.json"
    data = {"auth_token": auth_token, "value": value, "comment": comment}
    log_debug(f"PUT {url} data={data}")
    resp = requests.put(url, data=data, timeout=20)
    resp.raise_for_status()
    return resp.json()

def delete_datapoint(username: str, goal: str, auth_token: str, dp_id: str):
    if DRY_RUN:
        log_debug(f"[DRY_RUN] Would DELETE {goal} dp_id={dp_id}")
        return True
    url = f"{API_BASE}/users/{username}/goals/{goal}/datapoints/{dp_id}.json"
    params = {"auth_token": auth_token}
    log_debug(f"DELETE {url} params={params}")
    resp = requests.delete(url, params=params, timeout=20)
    resp.raise_for_status()
    return True

def parse_comment_for_length_and_time(comment: str):
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

def compute_today_outcome() -> int:
    today = today_daystamp(LOCAL_TZ)
    dps = get_datapoints_for_goal(USERNAME, FOCUSMATE_GOAL, AUTH_TOKEN)
    todays = [dp for dp in dps if dp.get("daystamp") == today]
    log_debug(f"Found {len(todays)} Focusmate datapoints for today.")

    for dp in todays:
        parsed = parse_comment_for_length_and_time(dp.get("comment", ""))
        if not parsed:
            continue
        minutes, hour, minute = parsed
        if minutes >= MIN_SESSION_MINUTES and qualifies_time(hour, minute):
            log_debug(f"Qualifying session found: {minutes} min at {hour}:{minute:02d}")
            return 1
    return 0

def sot_open():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(CREATE_SOT_SQL)
    conn.commit()
    return conn

def sot_get(conn: sqlite3.Connection, daystamp: str) -> int | None:
    cur = conn.execute(SELECT_SOT_SQL, (daystamp,))
    row = cur.fetchone()
    return int(row[0]) if row else None

def sot_set(conn: sqlite3.Connection, daystamp: str, value: int):
    log_debug(f"Setting SoT for {daystamp} to {value}")
    conn.execute(UPSERT_SOT_SQL, (daystamp, int(value), now_iso_utc()))
    conn.commit()

def reconcile_beeminder_with_sot(daystamp: str, sot_value: int):
    dps = get_datapoints_for_goal(USERNAME, WAKEANDFOCUS_GOAL, AUTH_TOKEN)
    todays = [dp for dp in dps if dp.get("daystamp") == daystamp]

    todays.sort(key=lambda dp: (dp.get("updated_at") or dp.get("timestamp") or 0), reverse=True)
    comment_ok = f"Auto: SoT={sot_value} for {daystamp} (≥50m by 09:15 check)."

    if not todays:
        requestid = f"{WAKEANDFOCUS_GOAL}-{daystamp}-sot-v1"
        add_datapoint(USERNAME, WAKEANDFOCUS_GOAL, AUTH_TOKEN, sot_value, comment_ok, requestid=requestid)
        return

    keeper = todays[0]
    extras = todays[1:]
    for dp in extras:
        delete_datapoint(USERNAME, WAKEANDFOCUS_GOAL, AUTH_TOKEN, dp["id"])

    try:
        keeper_value = float(keeper.get("value", 0))
    except Exception:
        keeper_value = 0.0

    if int(round(keeper_value)) != int(sot_value):
        update_datapoint(USERNAME, WAKEANDFOCUS_GOAL, AUTH_TOKEN, keeper["id"], sot_value, comment_ok)
    else:
        if (keeper.get("comment") or "") != comment_ok:
            update_datapoint(USERNAME, WAKEANDFOCUS_GOAL, AUTH_TOKEN, keeper["id"], sot_value, comment_ok)

def main():
    try:
        daystamp = today_daystamp(LOCAL_TZ)
        outcome = compute_today_outcome()
        conn = sot_open()
        sot_set(conn, daystamp, outcome)
        reconcile_beeminder_with_sot(daystamp, outcome)
    except requests.HTTPError as e:
        sys.stderr.write(f"HTTP error: {e} — {e.response.text if e.response else ''}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
