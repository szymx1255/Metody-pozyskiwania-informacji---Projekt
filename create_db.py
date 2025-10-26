"""Create SQLite schema for Open-Meteo data.

Tables created:
- fetches: metadata about each API fetch
- locations: list of locations (latitude, longitude, elevation, timezone)
- hourly: hourly observations per location and timestamp
- daily: daily summary per location and date

Run: python create_db.py
"""
import os
import sqlite3


DB_PATH = os.path.join("data", "meteodata.db")


def init_db(path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # Fetch metadata
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fetches (
            id INTEGER PRIMARY KEY,
            fetched_at TEXT,
            source TEXT,
            fetch_type TEXT,
            params TEXT,
            note TEXT
        )
        """
    )

    # Locations (one row per coordinate pair)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY,
            latitude REAL,
            longitude REAL,
            elevation REAL,
            timezone TEXT
        )
        """
    )

    # Hourly observations
    cur.execute(
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
        """
    )

    # Daily observations
    cur.execute(
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
        """
    )

    # Indexes for faster queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hourly_loc_time ON hourly(location_id, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_loc_date ON daily(location_id, date)")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Created DB at {DB_PATH}")
