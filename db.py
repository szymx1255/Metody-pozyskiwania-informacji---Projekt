import json
import os
import sqlite3
from typing import Dict, Iterable, List, Optional


DB_SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS fetches (
        id INTEGER PRIMARY KEY,
        fetched_at TEXT,
        source TEXT,
        fetch_type TEXT,
        params TEXT,
        note TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        elevation REAL,
        timezone TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hourly (
        id INTEGER PRIMARY KEY,
        location_id INTEGER,
        timestamp TEXT,
        temperature_2m REAL,
        rain REAL,
        showers REAL,
        snowfall REAL,
        snow_depth REAL,
        precipitation_probability REAL,
        visibility REAL,
        relative_humidity_2m REAL,
        wind_speed_10m REAL,
        wind_speed_80m REAL,
        wind_speed_120m REAL,
        wind_speed_180m REAL,
        FOREIGN KEY(location_id) REFERENCES locations(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily (
        id INTEGER PRIMARY KEY,
        location_id INTEGER,
        date TEXT,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        sunrise TEXT,
        sunset TEXT,
        uv_index_max REAL,
        precipitation_hours REAL,
        FOREIGN KEY(location_id) REFERENCES locations(id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_hourly_loc_time ON hourly(location_id, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_daily_loc_date ON daily(location_id, date)",
]


def init_db(path: str) -> None:
    """Utwórz strukturę bazy danych na dysku jeśli nie istnieje.

    Tworzy plik bazy (katalog jeśli potrzeba) i wykonuje schemat z DB_SCHEMA_SQL.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    for s in DB_SCHEMA_SQL:
        cur.execute(s)
    conn.commit()
    conn.close()


def insert_location(path: str, latitude: float, longitude: float, elevation: Optional[float] = None, timezone: Optional[str] = None) -> int:
    """Dodaj lokalizację lub zwróć istniejący identyfikator.

    Parametry:
      - path: ścieżka do pliku bazy
      - latitude, longitude: współrzędne
      - elevation, timezone: opcjonalne metadane
    Zwraca: id lokalizacji (int)
    """
    init_db(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # try to find existing
    cur.execute("SELECT id FROM locations WHERE latitude=? AND longitude=?", (latitude, longitude))
    row = cur.fetchone()
    if row:
        loc_id = row[0]
    else:
        cur.execute("INSERT INTO locations (latitude, longitude, elevation, timezone) VALUES (?, ?, ?, ?)", (latitude, longitude, elevation, timezone))
        loc_id = cur.lastrowid
        conn.commit()
    conn.close()
    return loc_id


def insert_hourly_bulk(path: str, location_id: int, rows: Iterable[Dict]) -> None:
    """Rows is iterable of dicts with keys matching hourly columns (timestamp, temperature_2m, ...)."""
    # Przygotuj i wstaw wiele wierszy do tabeli `hourly`.
    init_db(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    to_insert: List[tuple] = []
    for r in rows:
        to_insert.append(
            (
                location_id,
                r.get("timestamp"),
                r.get("temperature_2m"),
                r.get("rain"),
                r.get("showers"),
                r.get("snowfall"),
                r.get("snow_depth"),
                r.get("precipitation_probability"),
                r.get("visibility"),
                r.get("relative_humidity_2m"),
                r.get("wind_speed_10m"),
                r.get("wind_speed_80m"),
                r.get("wind_speed_120m"),
                r.get("wind_speed_180m"),
            )
        )
    cur.executemany(
        "INSERT INTO hourly (location_id, timestamp, temperature_2m, rain, showers, snowfall, snow_depth, precipitation_probability, visibility, relative_humidity_2m, wind_speed_10m, wind_speed_80m, wind_speed_120m, wind_speed_180m) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        to_insert,
    )
    conn.commit()
    conn.close()


def insert_daily_bulk(path: str, location_id: int, rows: Iterable[Dict]) -> None:
    # Wstaw wiele wierszy do tabeli `daily` (zbiorcze wartości dzienne).
    init_db(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    to_insert: List[tuple] = []
    for r in rows:
        to_insert.append(
            (
                location_id,
                r.get("date"),
                r.get("temperature_2m_max"),
                r.get("temperature_2m_min"),
                r.get("sunrise"),
                r.get("sunset"),
                r.get("uv_index_max"),
                r.get("precipitation_hours"),
            )
        )
    cur.executemany(
        "INSERT INTO daily (location_id, date, temperature_2m_max, temperature_2m_min, sunrise, sunset, uv_index_max, precipitation_hours) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        to_insert,
    )
    conn.commit()
    conn.close()


def save_fetch_meta(path: str, fetched_at: str, source: str, fetch_type: str, params: Optional[Dict] = None, note: Optional[str] = None) -> None:
    """Zapisz metadane o wykonanym pobraniu (np. czas, źródło, parametry).

    Przydatne do audytu i śledzenia historii fetchów.
    """
    init_db(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    params_json = json.dumps(params, ensure_ascii=False) if params is not None else None
    cur.execute("INSERT INTO fetches (fetched_at, source, fetch_type, params, note) VALUES (?, ?, ?, ?, ?)", (fetched_at, source, fetch_type, params_json, note))
    conn.commit()
    conn.close()
