from pathlib import Path
from datetime import datetime
from save_json import save_payload_to_json
from typing import Any, Dict, Iterable
import sqlite3
import logging
import requests
import time

DB_PATH = Path("data/meteodata.db")


# Wczytuje lokalizacje z bazy danych do pamieci przy starcie modulu
# Zwraca liste slownikow z nazwami i koordynatami lokalizacji
# Jesli wystapi blad zwraca pusta liste zamiast przerywac program
def _load_locations_from_db(db_path: Path) -> list:
    try:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        # Pobierz nazwy kolumn z pierwszego wiersza
        cur.execute("SELECT * FROM locations LIMIT 1")
        cols = [d[0] for d in cur.description] if cur.description else []
        # Znajdz indeksy kolumn zawierajacych dane lokalizacji
        name_idx = next((i for i, c in enumerate(cols) if "name" in c.lower()), None)
        lat_idx = next((i for i, c in enumerate(cols) if "lat" in c.lower()), None)
        lon_idx = next((i for i, c in enumerate(cols) if "lon" in c.lower()), None)
        id_idx = next((i for i, c in enumerate(cols) if c.lower() in ("id", "location_id", "loc_id")), None)

        # Pobierz wszystkie lokalizacje z bazy
        cur.execute("SELECT * FROM locations")
        rows = cur.fetchall()
        locations = []
        for r in rows:
            # Nazwa jest polem obowiazkowym
            name = None
            if name_idx is not None and name_idx < len(r):
                name = r[name_idx]
            else:
                # Jezeli nie ma indeksu znajdz pierwsze pole tekstowe
                for v in r:
                    if isinstance(v, str) and v.strip():
                        name = v
                        break
            if not name:
                continue
            loc = {"name": name}
            if id_idx is not None and id_idx < len(r):
                loc["id"] = r[id_idx]
            if lat_idx is not None and lat_idx < len(r):
                loc["latitude"] = r[lat_idx]
            if lon_idx is not None and lon_idx < len(r):
                loc["longitude"] = r[lon_idx]
            locations.append(loc)
        conn.close()
        return locations
    except Exception:
        logging.getLogger("meteofetch.api").exception("Nie udało się wczytać locations z DB (ignorowane)")
        try:
            conn.close()
        except Exception:
            pass
        return []


# Lista lokalizacji wczytana przy imporcie modulu
LOCATIONS = _load_locations_from_db(DB_PATH)

# Adres API Open-Meteo do pobierania prognoz pogodowych
API_URL = "https://api.open-meteo.com/v1/forecast"

# Domyslne parametry zapytania do API zawierajace zmienne pogodowe
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

# Zmienne dostepne w danych 15-minutowych
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
logger_api = logging.getLogger("meteofetch.api")


# Tworzy katalog data jesli nie istnieje
def ensure_dirs():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# Inicjalizuje strukture bazy danych tworząc wymagane tabele
# Dodaje kolumne origin do tabeli alerts jesli jej nie ma
def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    # Tabela z danymi lokalizacji
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
    # Tabela z danymi godzinowymi
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
    # Tabela z danymi 15-minutowymi
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
    # Tabela z alertami pogodowymi
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
    # Migracja dodajaca kolumne origin do starszych baz
    try:
        cur.execute("PRAGMA table_info(alerts)")
        cols = [r[1] for r in cur.fetchall()]
        if "origin" not in cols:
            cur.execute("ALTER TABLE alerts ADD COLUMN origin TEXT")
            conn.commit()
    except Exception:
        logger.debug("Brak potrzeby migracji tabeli alerts albo wystąpił błąd migracji.")


# Zwraca id lokalizacji z bazy lub dodaje nowa jezeli nie istnieje
def insert_or_get_location(conn: sqlite3.Connection, loc: dict) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO locations (name, latitude, longitude) VALUES (?, ?, ?)", (loc["name"], loc.get("latitude"), loc.get("longitude")))
    conn.commit()
    return cur.lastrowid


# Pobiera dane pogodowe z API Open-Meteo dla podanej lokalizacji
# Wykonuje do 3 prob z wykładniczym cofaniem w przypadku bledu
def fetch_location(location: dict) -> dict:
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


# Zapisuje dane godzinowe z payload do tabeli hourly
# Pomija duplikaty na podstawie znacznika czasu
# Zwraca liczbe dodanych wierszy
def store_hourly(conn: sqlite3.Connection, location_id: int, payload: dict) -> int:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return 0

    # Pomocnicza funkcja do bezpiecznego pobierania tablic z payload
    def arr(name):
        return hourly.get(name, [])

    temps = arr("temperature_2m")
    rains = arr("rain")
    snows = arr("snowfall")
    wind = arr("wind_speed_10m")
    codes = arr("weather_code") or arr("weathercode")
    dirs = arr("wind_direction_10m")
    uvs = arr("uv_index")

    # Pobierz najnowszy znacznik czasu z bazy aby uniknac duplikatow
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM hourly WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        # Pomijaj wiersze starsze lub rowne najnowszemu w bazie
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


# Zapisuje dane 15-minutowe z payload do tabeli minutely15
# Pomija duplikaty na podstawie znacznika czasu
# Zwraca liczbe dodanych wierszy
def store_minutely15(conn: sqlite3.Connection, location_id: int, payload: dict) -> int:
    minutely = payload.get("minutely_15", {})
    times = minutely.get("time", [])
    if not times:
        return 0

    # Pomocnicza funkcja do bezpiecznego pobierania tablic z payload
    def arr(name):
        return minutely.get(name, [])

    temps = arr("temperature_2m")
    wind = arr("wind_speed_10m")
    rains = arr("rain")
    snows = arr("snowfall")
    dirs = arr("wind_direction_10m")
    codes = arr("weather_code") or arr("weathercode")

    # Pobierz najnowszy znacznik czasu z bazy aby uniknac duplikatow
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM minutely15 WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        # Pomijaj wiersze starsze lub rowne najnowszemu w bazie
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


# Glowna funkcja pobierajaca i zapisujaca dane dla wybranych lokalizacji
# Obsluguje wybor lokalizacji typy danych i zapis plikow JSON
# Zwraca calkowita liczbe wstawionych wierszy do bazy
def fetch_and_store_all(db_path: Path | str,
                        fetch_hourly: bool,
                        fetch_minutely: bool,
                        location_names: Iterable[str] | None = None,
                        save_payloads: bool = False) -> int:
    logger_api.info("fetch_and_store_all called: hourly=%s minutely=%s locations=%s save_payloads=%s",
                fetch_hourly, fetch_minutely, location_names, save_payloads)
    
    if not fetch_hourly and not fetch_minutely:
        return 0
    
    ensure_dirs()
    conn = sqlite3.connect(str(db_path) if isinstance(db_path, Path) else db_path)
    init_db(conn)
    total = 0
    
    # Wybierz lokalizacje do pobrania lub wszystkie jesli nie podano
    locations_to_fetch = LOCATIONS
    if location_names:
        locations_to_fetch = [loc for loc in LOCATIONS if loc["name"] in location_names]
    
    for loc in locations_to_fetch:
        try:
            # Pobierz lub stworz id lokalizacji w bazie
            loc_id = insert_or_get_location(conn, loc)
            # Pobierz dane z API
            payload = fetch_location(loc)
            
            # Opcjonalnie zapisz surowy payload do pliku JSON
            if save_payloads:
                try:
                    save_payload_to_json(payload, prefix=loc["name"].replace(" ", "_"))
                except Exception:
                    logger.debug("Nie udało się zapisać payloadu do JSON dla %s", loc["name"])
            
            # Zapisz dane do odpowiednich tabel
            if fetch_hourly:
                total += store_hourly(conn, loc_id, payload)
            if fetch_minutely:
                total += store_minutely15(conn, loc_id, payload)
                
            logger.info("Pobrano dane dla %s", loc["name"])
        except Exception as e:
            logger.exception("Błąd podczas fetch/store dla %s: %s", loc["name"], e)
    
    conn.close()
    return total


if __name__ == "__main__":
    # Uruchomienie bezposrednie wykonuje pojedyncze pobranie danych
    inserted = fetch_and_store_all(DB_PATH, fetch_hourly=True, fetch_minutely=True, save_payloads=False)
    logger.info("Wstawiono rekordów: %d", inserted)