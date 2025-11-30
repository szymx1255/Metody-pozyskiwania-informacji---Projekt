from pathlib import Path
import os
import time
import json
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from http_client import get_client

LOGGER = logging.getLogger("meteofetch.api")
DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

# opcjonalny plik config.py zawierający LOCATIONS; format: [{'id':1,'name':'Zugspitze','lat':..., 'lon':...}, ...]
try:
    from config import LOCATIONS
except Exception:
    LOCATIONS = [
        {"id": 1, "name": "Zugspitze", "lat": 47.421, "lon": 10.985},
        {"id": 2, "name": "Grossglockner", "lat": 47.074, "lon": 12.695},
    ]

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
JSON_DIR = os.path.join(DATA_DIR, "json")
os.makedirs(JSON_DIR, exist_ok=True)

# requests session z prostym retry
_session = requests.Session()
_retries = Retry(total=3, backoff_factor=0.6, status_forcelist=(429,500,502,503,504))
_session.mount("https://", HTTPAdapter(max_retries=_retries))

MIN_REQUEST_INTERVAL = 0.5
_last_request = 0.0

def _throttle():
    global _last_request
    now = time.time()
    wait = MIN_REQUEST_INTERVAL - (now - _last_request)
    if wait > 0:
        time.sleep(wait)
    _last_request = time.time()

def _ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hourly (
        location_id INTEGER,
        timestamp TEXT,
        temperature REAL,
        rain REAL,
        snowfall REAL,
        wind_speed REAL,
        weather_code INTEGER,
        PRIMARY KEY(location_id, timestamp)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        timestamp TEXT,
        metric TEXT,
        value REAL,
        message TEXT,
        origin TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()

def _save_json(name: str, data: Dict[str, Any]) -> str:
    path = os.path.join(JSON_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def _fetch_open_meteo(lat: float, lon: float, start: Optional[str], end: Optional[str], hourly: List[str]) -> Dict[str, Any]:
    if start and end:
        url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": lat, "longitude": lon, "hourly": ",".join(hourly), "timezone": "UTC"}
    if start and end:
        params["start_date"] = start
        params["end_date"] = end
    _throttle()
    r = _session.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def _store_hourly(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any]) -> int:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    rains = hourly.get("rain", [])
    snows = hourly.get("snowfall", []) or hourly.get("snow_depth", [])
    winds = hourly.get("wind_speed_10m", [])
    codes = hourly.get("weathercode", []) or hourly.get("weather_code", [])
    cur = conn.cursor()
    inserted = 0
    for i, ts in enumerate(times):
        temp = temps[i] if i < len(temps) else None
        rain = rains[i] if i < len(rains) else 0.0
        snow = snows[i] if i < len(snows) else 0.0
        wind = winds[i] if i < len(winds) else None
        code = codes[i] if i < len(codes) else None
        try:
            cur.execute("""INSERT OR REPLACE INTO hourly
                (location_id,timestamp,temperature,rain,snowfall,wind_speed,weather_code)
                VALUES (?,?,?,?,?,?,?)""",
                (location_id, ts, temp, rain, snow, wind, code))
            inserted += 1
        except Exception:
            LOGGER.exception("Nie udało się zapisać wiersza hourly %s %s", location_id, ts)
    conn.commit()
    return inserted

def fetch_and_store_all(fetch_minutely: bool = False, fetch_hourly: bool = True,
                        start_date: Optional[str] = None, end_date: Optional[str] = None,
                        save_json: bool = False, locations: Optional[List[Dict[str,Any]]] = None) -> int:
    _ensure_db()
    if locations is None:
        locations = LOCATIONS
    total_inserted = 0
    conn = sqlite3.connect(DB_PATH)
    for loc in locations:
        name = loc.get("name") or str(loc.get("id"))
        LOGGER.info("Uruchamiam fetch (hourly=%s, minutely=%s) dla: %s", fetch_hourly, fetch_minutely, name)
        try:
            hourly_vars = ["temperature_2m","rain","snowfall","wind_speed_10m","weathercode"]
            payload = _fetch_open_meteo(loc["lat"], loc["lon"], start_date, end_date, hourly_vars if fetch_hourly else [])
            if save_json:
                fname = f"{name}_{start_date or 'now'}_{end_date or ''}_{int(time.time())}"
                _save_json(fname, payload)
            inserted = _store_hourly(conn, loc["id"], payload)
            total_inserted += inserted
        except Exception as e:
            LOGGER.exception("Błąd podczas fetch/store dla %s: %s", name, e)
    conn.close()
    return total_inserted


if __name__ == "__main__":
    # Prosty program: wykonaj jedno pobranie i zakończ.
    inserted = fetch_and_store_all(DB_PATH, fetch_hourly=True, fetch_minutely=True, save_payloads=False)
    logger.info("Wstawiono rekordów: %d", inserted)