from pathlib import Path
from datetime import datetime
from save_json import save_payload_to_json

import logging
import sqlite3
import requests
import os
import time
from typing import Any, Dict

DB_PATH = Path("data/meteodata.db")

# Lista lokalizacji do pobrania (nazwa, szerokość, długość, wysokość)
LOCATIONS = [
    {"name": "Grossglockner", "latitude": 47.0744, "longitude": 12.6940},
    {"name": "Täschhorn", "latitude": 46.0834, "longitude": 7.8572},
    {"name": "Zumsteinspitze", "latitude": 45.9322, "longitude": 7.8714},
    {"name": "Dufourspitze", "latitude": 45.9369, "longitude": 7.8668},
    {"name": "Mont Blanc", "latitude": 45.8330, "longitude": 6.8640},
    {"name": "Matterhorn", "latitude": 45.9764, "longitude": 7.6586},
    {"name": "Tryglaw", "latitude": 46.3782, "longitude": 13.8367},
    {"name": "Zugspitze", "latitude": 47.4212, "longitude": 10.9863},
]

# Parametry zapytania do API
API_URL = "https://api.open-meteo.com/v1/forecast"
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
    "past_days": 7,
    "forecast_days": 3,
    "timezone": "UTC",
}
MINUTELY_15_VARS = ",".join([
    "temperature_2m",
    "wind_speed_10m",
    "rain",
    "snowfall",
    "wind_direction_10m",
    "weather_code",
])

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("meteofetch")


def ensure_dirs():
    """Upewnij się, że katalog `data/` istnieje."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def init_db(conn: sqlite3.Connection):
    """Utwórz tabele `locations`, `hourly`, `minutely15` i `alerts` jeśli nie istnieją."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            latitude REAL,
            longitude REAL
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
            UNIQUE(location_id, timestamp)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS minutely15 (
            id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            wind_speed REAL,
            rain REAL,
            snowfall REAL,
            wind_direction REAL,
            weather_code INTEGER,
            UNIQUE(location_id, timestamp)
        )
        """
    )
    # Tabela alertów tworzona tutaj tylko po to, aby inny moduł mógł do niej zapisywać
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            timestamp TEXT,
            metric TEXT,
            value REAL,
            message TEXT,
            origin TEXT,
            UNIQUE(location_id, timestamp, metric, origin)
        )
        """
    )
    conn.commit()
    # migracja: jeśli tabela istnieje bez kolumny origin, spróbuj dodać
    try:
        cur.execute("PRAGMA table_info(alerts)")
        cols = [r[1] for r in cur.fetchall()]
        if "origin" not in cols:
            cur.execute("ALTER TABLE alerts ADD COLUMN origin TEXT")
            conn.commit()
    except Exception:
        logger.debug("Brak potrzeby migracji tabeli alerts albo wystąpił błąd migracji.")


def insert_or_get_location(conn: sqlite3.Connection, loc: dict) -> int:
    """Zwróć id lokalizacji; dodaj rekord jeśli nie istnieje."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO locations (name, latitude, longitude) VALUES (?, ?, ?)", (loc["name"], loc.get("latitude"), loc.get("longitude")))
    conn.commit()
    return cur.lastrowid


def fetch_location(location: dict) -> dict:
    """Pobierz dane z API Open-Meteo dla podanej lokalizacji (z retry/backoff)."""
    params = {"latitude": location["latitude"], "longitude": location["longitude"], **DEFAULT_PARAMS}
    params["minutely_15"] = MINUTELY_15_VARS
    attempts = 3
    backoff = 1
    for attempt in range(attempts):
        try:
            resp = requests.get(API_URL, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("Błąd pobierania dla %s (attempt %d/%d): %s", location["name"], attempt+1, attempts, e)
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(f"Nie udało się pobrać danych dla {location['name']}")


def store_hourly(conn: sqlite3.Connection, location_id: int, payload: dict) -> int:
    """Zapisz tablice `hourly` do tabeli `hourly`. Zwraca liczbę dodanych wierszy (potencjalnie zawiera zduplikowane próby)."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return 0

    def arr(name):
        return hourly.get(name, [])

    temps = arr("temperature_2m")
    rains = arr("rain")
    snows = arr("snowfall")
    wind = arr("wind_speed_10m")
    codes = arr("weather_code") or arr("weathercode")
    dirs = arr("wind_direction_10m")
    uvs = arr("uv_index")

    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM hourly WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        if max_ts is not None and t <= max_ts:
            continue
        t_temp = temps[i] if i < len(temps) else None
        t_rain = rains[i] if i < len(rains) else None
        t_snow = snows[i] if i < len(snows) else None
        t_wind = wind[i] if i < len(wind) else None
        t_code = codes[i] if i < len(codes) else None
        t_dir = dirs[i] if i < len(dirs) else None
        t_uv = uvs[i] if i < len(uvs) else None
        rows.append((location_id, t, t_temp, t_rain, t_snow, t_wind, t_code, t_dir, t_uv))

    if rows:
        cur.executemany(
            "INSERT OR IGNORE INTO hourly (location_id, timestamp, temperature, rain, snowfall, wind_speed, weather_code, wind_direction, uv_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
    return len(rows)


def store_minutely15(conn: sqlite3.Connection, location_id: int, payload: dict) -> int:
    """Zapisz dane 15-minutowe `minutely_15` do tabeli `minutely15`. Zwraca liczbę wierszy."""
    minutely = payload.get("minutely_15", {})
    times = minutely.get("time", [])
    if not times:
        return 0

    def arr(name):
        return minutely.get(name, [])

    temps = arr("temperature_2m")
    wind = arr("wind_speed_10m")
    rains = arr("rain")
    snows = arr("snowfall")
    dirs = arr("wind_direction_10m")
    codes = arr("weather_code") or arr("weathercode")

    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM minutely15 WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        if max_ts is not None and t <= max_ts:
            continue
        t_temp = temps[i] if i < len(temps) else None
        t_wind = wind[i] if i < len(wind) else None
        t_rain = rains[i] if i < len(rains) else None
        t_snow = snows[i] if i < len(snows) else None
        t_dir = dirs[i] if i < len(dirs) else None
        t_code = codes[i] if i < len(codes) else None
        rows.append((location_id, t, t_temp, t_wind, t_rain, t_snow, t_dir, t_code))

    if rows:
        cur.executemany(
            "INSERT OR IGNORE INTO minutely15 (location_id, timestamp, temperature, wind_speed, rain, snowfall, wind_direction, weather_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
    return len(rows)


def fetch_and_store_all(db_path: Path, *, fetch_hourly: bool = True, fetch_minutely: bool = True, location_names: list[str] | None = None, save_payloads: bool = False) -> int:
    """Pobierz i zapisz dane dla wskazanych lokalizacji.
    Zwraca łączną liczbę dodanych wierszy (hourly + minutely)."""
    if not fetch_hourly and not fetch_minutely:
        return 0

    ensure_dirs()
    conn = sqlite3.connect(db_path)
    init_db(conn)
    total = 0
    for loc in LOCATIONS:
        if location_names is not None and loc["name"] not in location_names:
            continue
        try:
            loc_id = insert_or_get_location(conn, loc)
            payload = fetch_location(loc)
            if save_payloads:
                try:
                    save_payload_to_json(payload, prefix=loc["name"].replace(" ", "_"))
                except Exception:
                    logger.debug("Nie udało się zapisać payloadu do JSON dla %s", loc["name"])
            if fetch_hourly:
                total += store_hourly(conn, loc_id, payload)
            if fetch_minutely:
                total += store_minutely15(conn, loc_id, payload)
        except Exception as e:
            logger.exception("Błąd podczas fetch/store dla %s: %s", loc["name"], e)
    conn.close()
    return total


if __name__ == "__main__":
    # Prosty program: wykonaj jedno pobranie i zakończ.
    inserted = fetch_and_store_all(DB_PATH, fetch_hourly=True, fetch_minutely=True, save_payloads=False)
    logger.info("Wstawiono rekordów: %d", inserted)