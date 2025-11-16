"""Moduł obsługi alertów opartych na danych historycznych i payloadach z API.

Funkcje:
 - query_historical_alerts(db_path): wypisuje rekordy z tabel `hourly` i `minutely15`
   które spełniają zdefiniowane warunki (wiatr, niska temp., opady).
 - analyze_payload_and_alert(conn, location_id, payload): analizuje surowy payload
   (np. po pobraniu z API) i wstawia alerty do tabeli `alerts` jeśli wykryje
   nadchodzące/obecne niekorzystne warunki.

Komentarze i komunikaty w języku polskim.
"""
from pathlib import Path
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Any

# Progi (synchroniczne z Api.py — zmień w obu miejscach jeśli chcesz inny próg)
ALERT_WIND_THRESHOLD = 58.0
ALERT_TEMP_LOW_THRESHOLD = -10.0
ALERT_WEATHER_CODES_PRECIP = {51, 53, 55, 61, 63, 65, 80, 81, 82, 95}

DATA_DIR = Path("data")
LOGGER = logging.getLogger("meteofetch")


def insert_alert_db(conn: sqlite3.Connection, location_id: int, timestamp: str | None, metric: str, value: float, message: str) -> None:
    """Wstaw alert do tabeli `alerts` (używane przez ten moduł)."""
    try:
        # wyznacz origin podobnie jak w Api.insert_alert
        origin = "detected"
        try:
            if timestamp:
                t_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                origin = "predicted" if t_dt > datetime.utcnow() else "historical"
        except Exception:
            pass
        cur = conn.cursor()
        # Wstawiamy też origin kolumnę (jeśli istnieje w schemacie będzie uzupełniona)
        cur.execute("INSERT INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)",
                    (location_id, timestamp, metric, value, message, origin))
        conn.commit()
        LOGGER.warning("ALERT (moduł Alert): %s (loc=%d) %s=%.2f", message, location_id, metric, value)
        return 1
    except Exception:
        LOGGER.exception("Nie udało się zapisać alertu do DB (Alert.insert_alert_db)")
        return 0


def _check_row_for_alerts(location_id: int, ts: str, temp, wind, rain, snow, code, conn: sqlite3.Connection) -> int:
    """Pomocnicza funkcja — sprawdza pojedynczy rekord i wstawia alerty jeśli trzeba."""
    added = 0
    try:
        if wind is not None:
            try:
                if float(wind) > ALERT_WIND_THRESHOLD:
                    added += insert_alert_db(conn, location_id, ts, "wind_speed", float(wind), f"Wiatr przekroczył {ALERT_WIND_THRESHOLD} m/s")
            except Exception:
                LOGGER.exception("Błąd przy sprawdzaniu wiatru (Alert._check_row)")
        if temp is not None:
            try:
                if float(temp) <= ALERT_TEMP_LOW_THRESHOLD:
                    added += insert_alert_db(conn, location_id, ts, "temperature", float(temp), f"Temperatura poniżej {ALERT_TEMP_LOW_THRESHOLD} °C")
            except Exception:
                LOGGER.exception("Błąd przy sprawdzaniu temperatury (Alert._check_row)")
        precip = False
        try:
            if rain is not None and float(rain) > 0:
                precip = True
            if snow is not None and float(snow) > 0:
                precip = True
        except Exception:
            LOGGER.exception("Błąd przy parsowaniu wartości opadów (Alert._check_row)")
        if not precip and code is not None:
            try:
                if int(code) in ALERT_WEATHER_CODES_PRECIP:
                    precip = True
            except Exception:
                pass
        if precip:
            added += insert_alert_db(conn, location_id, ts, "precipitation", float(rain or snow or 0.0), "Wykryto możliwe opady")
    except Exception:
        LOGGER.exception("Błąd przy sprawdzaniu pojedynczego wiersza (Alert._check_row)")
    return added


def analyze_payload_and_alert(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any]) -> int:
    """Analizuj surowy payload i wstaw alerty jeśli wykryto warunki.

    Zwraca liczbę wstawionych alertów w tej analizie.
    """
    added = 0
    try:
        # hourly
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        if times:
            def arr(name):
                return hourly.get(name, [])
            temps = arr("temperature_2m")
            winds = arr("wind_speed_10m")
            rains = arr("rain")
            snows = arr("snowfall")
            codes = arr("weather_code") or arr("weathercode")
            for i, ts in enumerate(times):
                # tylko przyszłe lub bieżące czasy
                try:
                    t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    t_dt = None
                now = datetime.utcnow()
                if t_dt and t_dt < now:
                    # pomijamy przeszłe elementy w analizie payload (historyczne będą w DB)
                    continue
                temp = temps[i] if i < len(temps) else None
                wind = winds[i] if i < len(winds) else None
                rain = rains[i] if i < len(rains) else None
                snow = snows[i] if i < len(snows) else None
                code = codes[i] if i < len(codes) else None
                added += _check_row_for_alerts(location_id, ts, temp, wind, rain, snow, code, conn)
                # we can't easily know how many were added per row without querying; skip precise count
        # minutely_15
        minutely = payload.get("minutely_15", {})
        times = minutely.get("time", [])
        if times:
            def arr2(name):
                return minutely.get(name, [])
            temps = arr2("temperature_2m")
            winds = arr2("wind_speed_10m")
            rains = arr2("rain")
            snows = arr2("snowfall")
            codes = arr2("weather_code") or arr2("weathercode")
            for i, ts in enumerate(times):
                try:
                    t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    t_dt = None
                now = datetime.utcnow()
                if t_dt and t_dt < now:
                    continue
                temp = temps[i] if i < len(temps) else None
                wind = winds[i] if i < len(winds) else None
                rain = rains[i] if i < len(rains) else None
                snow = snows[i] if i < len(snows) else None
                code = codes[i] if i < len(codes) else None
                added += _check_row_for_alerts(location_id, ts, temp, wind, rain, snow, code, conn)
    except Exception:
        LOGGER.exception("Błąd podczas analizy payloadu pod kątem alertów")
    return added


def query_historical_alerts(db_path: str | Path) -> List[Dict[str, Any]]:
    """Pobierz z bazy wszystkie rekordy historyczne które spełniają warunki alertowe.

    Zwraca listę rekordów z tabel `hourly` i `minutely15`.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    results: List[Dict[str, Any]] = []
    try:
        # hourly
        cur.execute(
            f"SELECT 'hourly' as table_name, location_id, timestamp, temperature, wind_speed, rain, snowfall, weather_code FROM hourly WHERE "
            f"(wind_speed > ?) OR (temperature <= ?) OR (rain > 0) OR (snowfall > 0) OR (weather_code IN ({','.join(['?']*len(ALERT_WEATHER_CODES_PRECIP))}))",
            tuple([ALERT_WIND_THRESHOLD, ALERT_TEMP_LOW_THRESHOLD] + list(ALERT_WEATHER_CODES_PRECIP))
        )
        cols = [d[0] for d in cur.description]
        for r in cur.fetchall():
            results.append(dict(zip(cols, r)))

        # minutely15
        cur.execute(
            f"SELECT 'minutely15' as table_name, location_id, timestamp, temperature, wind_speed, rain, snowfall, weather_code FROM minutely15 WHERE "
            f"(wind_speed > ?) OR (temperature <= ?) OR (rain > 0) OR (snowfall > 0) OR (weather_code IN ({','.join(['?']*len(ALERT_WEATHER_CODES_PRECIP))}))",
            tuple([ALERT_WIND_THRESHOLD, ALERT_TEMP_LOW_THRESHOLD] + list(ALERT_WEATHER_CODES_PRECIP))
        )
        cols2 = [d[0] for d in cur.description]
        for r in cur.fetchall():
            results.append(dict(zip(cols2, r)))
    except Exception:
        LOGGER.exception("Błąd przy zapytaniu historycznych alertów")
    finally:
        conn.close()
    return results


if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser(description="Narzędzie do sprawdzania alertów historycznych")
    p.add_argument("--db", default=str(DATA_DIR / "meteodata.db"), help="Ścieżka do pliku DB")
    p.add_argument("--show", action="store_true", help="Wyświetl historyczne rekordy spełniające warunki")
    args = p.parse_args()
    if args.show:
        recs = query_historical_alerts(args.db)
        print(json.dumps(recs, ensure_ascii=False, indent=2))
