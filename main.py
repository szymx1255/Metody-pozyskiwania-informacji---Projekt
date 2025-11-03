import argparse
import os
import signal
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
import requests


DB_PATH = Path("data/meteodata.db")

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
    # zmienione dla minutely_15: krótszy zakres historii (7 dni)
    "past_days": 7,
    "forecast_days": 3,
    "timezone": "UTC",
}

# Support for 15-minute resolution variables (minutely_15)
MINUTELY_15_VARS = ",".join([
    "temperature_2m",
    "wind_speed_10m",
    "rain",
    "snowfall",
    "wind_direction_10m",
    "weather_code",
])


def ensure_dirs():
    # Utwórz katalog (data/) jeśli nie istnieje
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


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
    # PL: Zainicjalizuj tabele w bazie danych: locations i hourly.
    # Tabela `hourly` ma unikalny klucz (location_id, timestamp),
    # co ułatwia pomijanie duplikatów przy wstawianiu.
    # Dodaj tabelę na dane 15-minutowe (minutely_15)
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
    # PL: Jeśli lokalizacja istnieje, zwróć jej id, w przeciwnym razie dodaj nowy rekord.


def fetch_location(location: dict, extra_params: dict | None = None) -> dict:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        **DEFAULT_PARAMS,
    }
    # Dołącz parametr minutely_15 aby API zwróciło 15-minutowe pola (jeśli obsługiwane)
    params["minutely_15"] = MINUTELY_15_VARS
    if extra_params:
        params.update(extra_params)
    # PL: Wykonaj zapytanie HTTP do API Open-Meteo z prostym retry/backoff
    print(f"[HTTP] Fetching {location['name']} ({location['latitude']},{location['longitude']})")
    attempts = 3
    backoff = 1
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            payload = r.json()
            return payload
        except requests.HTTPError as he:
            # For 4xx errors, don't retry (client error), return/raise immediately
            print(f"[HTTP] HTTP error for {location['name']}: {he}")
            raise
        except requests.RequestException as re:
            print(f"[HTTP] transient error (attempt {attempt}/{attempts}) for {location['name']}: {re}")
            last_exc = re
            time.sleep(backoff)
            backoff *= 2
    # If all retries failed, raise last exception
    raise last_exc

    # PL: Payload zawiera pola 'hourly' z tablicami czasów i wartości.


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

    # PL: Znajdź najnowszy zapisany timestamp dla lokalizacji, aby nie duplikować
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM hourly WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        # PL: Pomiń godziny które już mamy w bazie (<= max_ts)
        if max_ts is not None and t <= max_ts:
            continue
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

    # cur already defined above when checking max_ts
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
    # PL: Zwracamy liczbę wstawionych/ zaktualizowanych wierszy godzinowych.


def store_minutely15(conn: sqlite3.Connection, location_id: int, location_name: str, payload: dict) -> int:
    """Zapisz dane 15-minutowe z pola 'minutely_15' (jeśli obecne).

    PL: Funkcja filtruje już zapisane timestampy (MAX per location) i wstawia tylko nowe.
    """
    minutely = payload.get("minutely_15") or {}
    times = minutely.get("time", [])
    if not times:
        # brak minutely_15 w odpowiedzi
        return 0

    def get_arr(name):
        return minutely.get(name, [])

    temps = get_arr("temperature_2m")
    wind_speeds = get_arr("wind_speed_10m")
    rains = get_arr("rain")
    snows = get_arr("snowfall")
    wind_dirs = get_arr("wind_direction_10m")
    weather_codes = get_arr("weather_code") or get_arr("weathercode")

    # unikaj duplikatów: pobierz maksymalny timestamp dla tej lokalizacji
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM minutely15 WHERE location_id=?", (location_id,))
    r = cur.fetchone()
    max_ts = r[0] if r and r[0] is not None else None

    rows = []
    for i, t in enumerate(times):
        if max_ts is not None and t <= max_ts:
            continue
        rows.append(
            (
                location_id,
                t,
                temps[i] if i < len(temps) else None,
                wind_speeds[i] if i < len(wind_speeds) else None,
                rains[i] if i < len(rains) else None,
                snows[i] if i < len(snows) else None,
                wind_dirs[i] if i < len(wind_dirs) else None,
                weather_codes[i] if i < len(weather_codes) else None,
            )
        )

    cur.executemany(
        """
        INSERT OR REPLACE INTO minutely15
            (location_id, timestamp, temperature, wind_speed, rain, snowfall, wind_direction, weather_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

            # check latest stored timestamp for this location
            cur = conn.cursor()
            cur.execute("SELECT MAX(timestamp) FROM hourly WHERE location_id=?", (loc_id,))
            r = cur.fetchone()
            max_ts = r[0] if r and r[0] is not None else None

            # Jeśli mamy już dane godzinowe do aktualnej godziny, sprawdź też tabelę 15-minutową.
            # Jeśli zarówno hourly jak i minutely15 są aktualne (mają timestamp >= bieżącej godziny),
            # pomiń pobieranie. W przeciwnym razie pobierz (np. żeby uzupełnić brakujące minutely15).
            now = datetime.utcnow()
            current_hour_iso = now.replace(minute=0, second=0, microsecond=0).isoformat()

            cur2 = conn.cursor()
            cur2.execute("SELECT MAX(timestamp) FROM minutely15 WHERE location_id=?", (loc_id,))
            r2 = cur2.fetchone()
            max_ts_minutely = r2[0] if r2 and r2[0] is not None else None

            if max_ts is not None and max_ts >= current_hour_iso and max_ts_minutely is not None and max_ts_minutely >= current_hour_iso:
                print(f"[SKIP] {loc['name']} is fully up-to-date (hourly_max={max_ts} minutely15_max={max_ts_minutely})")
                continue

            # Nie ustawiamy start_date — unikamy wysyłania kombinacji parametrów,
            # które w niektórych sytuacjach powodowały 400 (API może nie akceptować
            # start_date razem z innymi parametrami w tej formie). Pobieramy pełny
            # zakres (past_days) i polegamy na deduplikacji przed zapisem.
            extra = None

            # PL: Pobierz nowe dane (od start_date jeśli było max_ts)
            payload = fetch_location(loc, extra_params=extra)
            # store both hourly and 15-minute (if present)
            inserted_h = store_hourly(conn, loc_id, loc["name"], payload)
            inserted_m = store_minutely15(conn, loc_id, loc["name"], payload)
            total_inserted += (inserted_h + inserted_m)
            print(f"[DB] {loc['name']}: hourly={inserted_h} rows, minutely15={inserted_m} rows (location_id={loc_id})")
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
    # PL: Reaguj na SIGINT i SIGTERM i ustaw stop_event aby przerwać pętlę po bieżącej iteracji.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run a single fetch+store then exit")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to sqlite DB file")
    args = parser.parse_args()

    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    # PL: Flaga --once pozwala na jednorazowe uruchomienie (przydatne do testów)
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