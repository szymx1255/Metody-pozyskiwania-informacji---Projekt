from pathlib import Path
import sqlite3
import requests
import time
from datetime import datetime
import logging
import os
from save_json import save_payload_to_json
import Alert


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

# Proste progi alertów (można zmienić)
# Uwaga: wybrałem wartości domyślne; jeśli chcesz inne, powiedz a je zmienię.
ALERT_WIND_THRESHOLD = 58.0  # m/s - próg wysoki wiatr (zgodnie z prośbą)
ALERT_TEMP_LOW_THRESHOLD = -10.0  # °C - przyjmujemy, że poniżej tej wartości alarmujemy
# Jeżeli występują opady deszczu/sniegu > 0 lub weather_code wskazuje opady -> alert
ALERT_WEATHER_CODES_PRECIP = {51, 53, 55, 61, 63, 65, 80, 81, 82, 95}


def insert_alert(conn: sqlite3.Connection, location_id: int, timestamp: str | None, metric: str, value: float, message: str, origin: str | None = None) -> None:
    """Wstaw prosty alert do tabeli `alerts`.

    Jeśli `origin` nie zostanie podany, próbujemy je wyznaczyć z pola `timestamp`:
      - jeśli timestamp > teraz => origin='predicted'
      - w przeciwnym razie => origin='historical'
      - jeśli parsowanie się nie powiedzie => origin='detected'
    """
    try:
        # wyznacz origin jeśli nie podano
        if origin is None:
            origin = "detected"
            try:
                if timestamp:
                    t_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    origin = "predicted" if t_dt > datetime.utcnow() else "historical"
            except Exception:
                pass
        cur = conn.cursor()
        cur.execute("INSERT INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)", (location_id, timestamp, metric, value, message, origin))
        conn.commit()
        logger.warning("ALERT: %s (loc=%d) %s=%.2f origin=%s", message, location_id, metric, value, origin)
    except Exception:
        logger.exception("Nie udało się zapisać alertu")
    # Opcjonalne wysłanie powiadomienia przez webhook (zmienna środowiskowa ALERT_WEBHOOK_URL)
    try:
        webhook = os.environ.get("ALERT_WEBHOOK_URL")
        if webhook:
            payload = {
                "location_id": location_id,
                "timestamp": timestamp,
                "metric": metric,
                "value": value,
                "message": message,
                "origin": origin,
            }
            # bezpieczne wysłanie POST, nie blokujemy głównego procesu na długi czas
            try:
                requests.post(webhook, json=payload, timeout=5)
            except Exception:
                logger.exception("Nie udało się wysłać powiadomienia webhook")
    except Exception:
        # Nie dopuszczamy żeby błąd powiadomienia przerwał logikę zapisu
        logger.exception("Błąd przy próbie przygotowania powiadomienia")


def ensure_dirs():
    """Upewnij się, że katalog `data/` istnieje."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def init_db(conn: sqlite3.Connection):
    """Utwórz tabele `locations`, `hourly`, `minutely15` jeśli nie istnieją."""
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
    # Tabela alertów na nietypowe dane (np. zbyt duży wiatr)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY,
            location_id INTEGER NOT NULL,
            timestamp TEXT,
            metric TEXT,
            value REAL,
            message TEXT,
            origin TEXT
        )
        """
    )
    conn.commit()
    # migracja: jeśli tabela istnieje bez kolumny origin, dodajemy ją
    try:
        cur.execute("PRAGMA table_info(alerts)")
        existing = [r[1] for r in cur.fetchall()]
        if "origin" not in existing:
            cur.execute("ALTER TABLE alerts ADD COLUMN origin TEXT")
            conn.commit()
    except Exception:
        logger.exception("Nie udało się sprawdzić/migrować tabeli alerts")


def insert_or_get_location(conn: sqlite3.Connection, loc: dict) -> int:
    """Zwróć id lokalizacji; dodaj rekord jeśli nie istnieje."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO locations (name, latitude, longitude) VALUES (?, ?, ?)", (loc["name"], loc["latitude"], loc["longitude"]))
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
            logger.info("Pobieram %s", location["name"])
            r = requests.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning("Błąd sieci dla %s (attempt %d): %s", location["name"], attempt + 1, e)
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(f"Nie udało się pobrać danych dla {location['name']}")


def store_hourly(conn: sqlite3.Connection, location_id: int, payload: dict) -> int:
    """Zapisz tablice `hourly` do tabeli `hourly`. Zwraca liczbę wstawionych wierszy."""
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
        # Obsługa brakujących danych: jeśli wartość jest None, spróbuj użyć ostatniej znanej wartości z bazy
        def last_or(value, col_name):
            if value is not None:
                return value
            cur2 = conn.cursor()
            try:
                cur2.execute(f"SELECT {col_name} FROM hourly WHERE location_id=? ORDER BY timestamp DESC LIMIT 1", (location_id,))
                row2 = cur2.fetchone()
                return row2[0] if row2 else None
            finally:
                cur2.close()

        t_temp = last_or(temps[i] if i < len(temps) else None, "temperature")
        t_rain = last_or(rains[i] if i < len(rains) else None, "rain")
        t_snow = last_or(snows[i] if i < len(snows) else None, "snowfall")
        t_wind = last_or(wind[i] if i < len(wind) else None, "wind_speed")
        t_code = codes[i] if i < len(codes) else None
        t_dir = dirs[i] if i < len(dirs) else None
        t_uv = uvs[i] if i < len(uvs) else None
        rows.append((location_id, t, t_temp, t_rain, t_snow, t_wind, t_code, t_dir, t_uv))
        # Alerty: wykryj nietypowe wartości
        try:
            # wiatr
            if t_wind is not None and float(t_wind) > ALERT_WIND_THRESHOLD:
                insert_alert(conn, location_id, t, "wind_speed", float(t_wind), f"Wiatr przekroczył {ALERT_WIND_THRESHOLD} m/s")
            # niska temperatura
            if t_temp is not None:
                try:
                    if float(t_temp) <= ALERT_TEMP_LOW_THRESHOLD:
                        insert_alert(conn, location_id, t, "temperature", float(t_temp), f"Temperatura poniżej {ALERT_TEMP_LOW_THRESHOLD} °C")
                except Exception:
                    logger.exception("Błąd przy sprawdzaniu progu temperatury (hourly)")
            # opady: jeśli mamy bezwzględne wartości deszczu/śniegu > 0 lub weather_code wskazuje opady
            try:
                precip = False
                if t_rain is not None and float(t_rain) > 0:
                    precip = True
                if t_snow is not None and float(t_snow) > 0:
                    precip = True
                if t_code is not None:
                    try:
                        if int(t_code) in ALERT_WEATHER_CODES_PRECIP:
                            precip = True
                    except Exception:
                        pass
                if precip:
                    insert_alert(conn, location_id, t, "precipitation", float(t_rain or t_snow or 0.0), "Wykryto możliwe opady")
            except Exception:
                logger.exception("Błąd przy sprawdzaniu opadów (hourly)")
        except Exception:
            logger.exception("Błąd przy sprawdzaniu alertów (hourly)")
    if rows:
        cur.executemany("INSERT OR REPLACE INTO hourly (location_id, timestamp, temperature, rain, snowfall, wind_speed, weather_code, wind_direction, uv_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
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
        # Obsługa brakujących danych analogicznie do hourly
        def last_or(value, col_name):
            if value is not None:
                return value
            cur2 = conn.cursor()
            try:
                cur2.execute(f"SELECT {col_name} FROM minutely15 WHERE location_id=? ORDER BY timestamp DESC LIMIT 1", (location_id,))
                row2 = cur2.fetchone()
                return row2[0] if row2 else None
            finally:
                cur2.close()

        t_temp = last_or(temps[i] if i < len(temps) else None, "temperature")
        t_wind = last_or(wind[i] if i < len(wind) else None, "wind_speed")
        t_rain = last_or(rains[i] if i < len(rains) else None, "rain")
        t_snow = last_or(snows[i] if i < len(snows) else None, "snowfall")
        t_dir = dirs[i] if i < len(dirs) else None
        t_code = codes[i] if i < len(codes) else None

        rows.append((location_id, t, t_temp, t_wind, t_rain, t_snow, t_dir, t_code))
        # Alerty analogiczne do hourly
        try:
            if t_wind is not None and float(t_wind) > ALERT_WIND_THRESHOLD:
                insert_alert(conn, location_id, t, "wind_speed", float(t_wind), f"Wiatr przekroczył {ALERT_WIND_THRESHOLD} m/s")
            if t_temp is not None:
                try:
                    if float(t_temp) <= ALERT_TEMP_LOW_THRESHOLD:
                        insert_alert(conn, location_id, t, "temperature", float(t_temp), f"Temperatura poniżej {ALERT_TEMP_LOW_THRESHOLD} °C")
                except Exception:
                    logger.exception("Błąd przy sprawdzaniu progu temperatury (minutely)")
            try:
                precip = False
                if t_rain is not None and float(t_rain) > 0:
                    precip = True
                if t_snow is not None and float(t_snow) > 0:
                    precip = True
                if t_code is not None:
                    try:
                        if int(t_code) in ALERT_WEATHER_CODES_PRECIP:
                            precip = True
                    except Exception:
                        pass
                if precip:
                    insert_alert(conn, location_id, t, "precipitation", float(t_rain or t_snow or 0.0), "Wykryto możliwe opady")
            except Exception:
                logger.exception("Błąd przy sprawdzaniu opadów (minutely)")
        except Exception:
            logger.exception("Błąd przy sprawdzaniu alertów (minutely)")
    if rows:
        cur.executemany("INSERT OR REPLACE INTO minutely15 (location_id, timestamp, temperature, wind_speed, rain, snowfall, wind_direction, weather_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    return len(rows)


def fetch_and_store_all(db_path: Path, *, fetch_hourly: bool = True, fetch_minutely: bool = True, location_names: list[str] | None = None, save_payloads: bool = False) -> int:
    """Pobierz i zapisz dane dla wskazanych lokalizacji.

    Parametry:
      - db_path: ścieżka do pliku SQLite
      - fetch_hourly: czy zapisać dane godzinowe
      - fetch_minutely: czy zapisać dane 15-minutowe
      - location_names: opcjonalna lista nazw lokalizacji do pobrania (jeśli None => wszystkie)

    Zwraca liczbę wstawionych/ zaktualizowanych wierszy.
    """
    if not fetch_hourly and not fetch_minutely:
        raise ValueError("Przynajmniej jedna z opcji fetch_hourly lub fetch_minutely musi być True")

    ensure_dirs()
    conn = sqlite3.connect(db_path)
    init_db(conn)
    total = 0
    for loc in LOCATIONS:
        if location_names is not None and loc.get("name") not in location_names:
            continue
        loc_id = insert_or_get_location(conn, loc)
        payload = fetch_location(loc)
        # Analiza payloadu pod kątem alertów (np. nadchodzące/obecne warunki)
        try:
            alerts_added = Alert.analyze_payload_and_alert(conn, loc_id, payload)
            if alerts_added:
                logger.info("Wygenerowano %d alertów z analizy payloadu dla %s", alerts_added, loc.get("name"))
        except Exception:
            logger.exception("Błąd przy analizie payloadu pod kątem alertów")

        # opcjonalnie zapisz surowy payload do pliku JSON na dysku
        if save_payloads:
            try:
                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                safe_name = loc.get("name", "unknown").replace(" ", "_")
                fname = f"{safe_name}-{ts}.json"
                save_payload_to_json(payload, filename=fname, prefix=safe_name)
                logger.info("Zapisano payload do %s", fname)
            except Exception:
                logger.exception("Nie udało się zapisać payloadu do JSON")

        if fetch_hourly:
            total += store_hourly(conn, loc_id, payload)
        if fetch_minutely:
            total += store_minutely15(conn, loc_id, payload)
    conn.close()
    return total


if __name__ == "__main__":
    # Prosty program: wykonaj jedno pobranie i zakończ.
    inserted = fetch_and_store_all(DB_PATH)
    logger.info("Wstawiono/ zaktualizowano %d wierszy", inserted)