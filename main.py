#!/usr/bin/env python3
"""Fetch hourly weather for a set of mountains every hour and store into SQLite.

API: https://api.open-meteo.com/v1/forecast
Requested params: past_days=31, forecast_days=3, hourly=temperature_2m,rain,snowfall,wind_speed_10m,weather_code,wind_direction_10m,uv_index

This script stores raw JSON and normalized hourly rows in `data/meteodata.db`.
"""

import argparse
import json
import os
import signal
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

# DB file
DB_PATH = Path("data/meteodata.db")
RAW_DIR = Path("data/raw")

# API config
API_URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT = 30

# Coordinates/order taken from your provided API URL (8 locations)
# Map index -> mountain name taken from your screenshot (order aligns with coords below)
LOCATIONS = [
    {"name": "Grossglockner", "latitude": 47.0744, "longitude": 12.6940, "elevation_m": 3798},
    {"name": "Täschhorn", "latitude": 46.0834, "longitude": 7.8572, "elevation_m": 4491},
    {"name": "Zumsteinspitze", "latitude": 45.9322, "longitude": 7.8714, "elevation_m": 4563},
    {"name": "Dufourspitze", "latitude": 45.9369, "longitude": 7.8668, "elevation_m": 4634},
    {"name": "Mont Blanc", "latitude": 45.8330, "longitude": 6.8640, "elevation_m": 4806},
    {"name": "Matterhorn", "latitude": 45.9764, "longitude": 7.6586, "elevation_m": 4478},
    {"name": "Tryglaw", "latitude": 46.3782, "longitude": 13.8367, "elevation_m": 2864},
    {"name": "Zugspitze", "latitude": 47.4212, "longitude": 10.9863, "elevation_m": 2962},
]

DEFAULT_PARAMS = {
    "hourly": ",".join([
        "temperature_2m",
        "rain",
        "snowfall",
        "wind_speed_10m",
        "weather_code",
        "wind_direction_10m",
        "uv_index",
    ]),
    "past_days": 31,
    "forecast_days": 3,
    "timezone": "UTC",
}


def ensure_dirs():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            latitude REAL,
            longitude REAL,
            elevation_m REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hourly (
            id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            rain REAL,
            snowfall REAL,
            wind_speed REAL,
            weather_code INTEGER,
            wind_direction REAL,
            uv_index REAL,
            UNIQUE(location_id, timestamp),
            FOREIGN KEY(location_id) REFERENCES locations(id)
        )
        """
    )
    conn.commit()


def insert_or_get_location(conn: sqlite3.Connection, loc: dict) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO locations (name, latitude, longitude, elevation_m) VALUES (?, ?, ?, ?)",
        (loc["name"], loc["latitude"], loc["longitude"], loc.get("elevation_m")),
    )
    conn.commit()
    return cur.lastrowid


def fetch_location(location: dict) -> dict:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        **DEFAULT_PARAMS,
    }
    print(f"[HTTP] Fetching {location['name']} ({location['latitude']},{location['longitude']})")
    r = requests.get(API_URL, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    # save raw JSON for audit
    fname = RAW_DIR / f"{location['name'].replace(' ', '_')}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[FILE] saved raw response: {fname}")
    return payload


def store_hourly(conn: sqlite3.Connection, location_id: int, location_name: str, payload: dict) -> int:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time", [])
    if not times:
        print(f"[WARN] no hourly times for {location_name}")
        return 0

    def get_arr(name):
        return hourly.get(name, [])

    temps = get_arr("temperature_2m")
    rains = get_arr("rain")
    snows = get_arr("snowfall")
    wind_speeds = get_arr("wind_speed_10m")
    weather_codes = get_arr("weather_code") or get_arr("weathercode")
    wind_dirs = get_arr("wind_direction_10m")
    uvs = get_arr("uv_index")

    rows = []
    for i, t in enumerate(times):
        rows.append(
            (
                location_id,
                t,
                temps[i] if i < len(temps) else None,
                rains[i] if i < len(rains) else None,
                snows[i] if i < len(snows) else None,
                wind_speeds[i] if i < len(wind_speeds) else None,
                weather_codes[i] if i < len(weather_codes) else None,
                wind_dirs[i] if i < len(wind_dirs) else None,
                uvs[i] if i < len(uvs) else None,
            )
        )

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO hourly
            (location_id, timestamp, temperature, rain, snowfall, wind_speed, weather_code, wind_direction, uv_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def fetch_and_store_all(db_path: Path):
    ensure_dirs()
    conn = sqlite3.connect(db_path)
    init_db(conn)
    total_inserted = 0
    for loc in LOCATIONS:
        try:
            loc_id = insert_or_get_location(conn, loc)
            payload = fetch_location(loc)
            inserted = store_hourly(conn, loc_id, loc["name"], payload)
            total_inserted += inserted
            print(f"[DB] {loc['name']}: inserted/updated {inserted} hourly rows (location_id={loc_id})")
        except Exception as e:
            print(f"[ERROR] Failed for {loc['name']}: {e}")
            # continue with next location
    conn.close()
    return total_inserted


def _install_signal_handlers(stop_event: threading.Event):
    def _handler(signum, frame):
        print(f"[SIGNAL] Received {signum}. Stopping after current iteration...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run a single fetch+store then exit")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to sqlite DB file")
    args = parser.parse_args()

    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    if args.once:
        print("[RUN] single run (--once)")
        inserted = fetch_and_store_all(Path(args.db))
        print(f"[DONE] inserted/updated {inserted} total rows")
        return

    print("[RUN] starting continuous hourly fetch. Press Ctrl+C to stop.")
    while not stop_event.is_set():
        start = time.time()
        try:
            inserted = fetch_and_store_all(Path(args.db))
            print(f"[LOOP] iteration done. inserted/updated {inserted} rows. time={time.time()-start:.1f}s")
        except Exception as e:
            print(f"[ERROR] During iteration: {e}")
        # wait up to 1 hour from start, but exit early if stop_event set
        wait_seconds = max(0, 3600 - (time.time() - start))
        if stop_event.wait(timeout=wait_seconds):
            break


if __name__ == "__main__":
    main()