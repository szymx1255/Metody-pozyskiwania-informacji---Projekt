import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

ALERT_WIND_THRESHOLD = 58.0
ALERT_TEMP_LOW_THRESHOLD = -19.0
ALERT_WEATHER_CODES_PRECIP = {51, 53, 55, 61, 63, 65, 80, 81, 82, 95}

LOGGER = logging.getLogger("meteofetch")


def _extract_hour(ts: str) -> str:
    """Wyciąga HH:MM z timestampu ISO (np. '2025-11-18T14:00' -> '14:00')."""
    try:
        if "T" not in ts:
            return "?"
        return ts.split("T")[1][:5]
    except Exception:
        return "?"


def insert_alert_db(conn: sqlite3.Connection, location_id: int, timestamp: str | None,
                    metric: str, value: float, message: str) -> int:
    """Wstaw alert do tabeli `alerts` jeśli nie istnieje."""
    try:
        origin = "detected"
        try:
            if timestamp:
                t_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                origin = "predicted" if t_dt > datetime.utcnow() else "historical"
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)",
            (location_id, timestamp, metric, value, message, origin)
        )
        conn.commit()
        if cur.rowcount:
            LOGGER.warning("ALERT: %s (loc=%d) %s=%.2f", message, location_id, metric, value)
            return 1
        return 0
    except Exception:
        LOGGER.exception("Nie udało się zapisać alertu do DB")
        return 0


def _check_row_for_alerts(location_id: int, ts: str, temp, wind, rain, snow, code,
                          conn: sqlite3.Connection) -> int:
    """Sprawdza pojedynczy rekord i zapisuje alerty jeśli trzeba."""
    added = 0
    try:
        hour_str = _extract_hour(ts)

        if wind is not None and float(wind) > ALERT_WIND_THRESHOLD:
            added += insert_alert_db(conn, location_id, ts, "wind_speed", float(wind),
                                     f"Wiatr przekroczył {ALERT_WIND_THRESHOLD} m/s o {hour_str}")
        if temp is not None and float(temp) <= ALERT_TEMP_LOW_THRESHOLD:
            added += insert_alert_db(conn, location_id, ts, "temperature", float(temp),
                                     f"Temperatura poniżej {ALERT_TEMP_LOW_THRESHOLD} °C o {hour_str}")
        precip = False
        pr_value = 0.0
        if rain is not None and float(rain) > 0:
            precip = True
            pr_value = float(rain)
        if snow is not None and float(snow) > 0:
            precip = True
            pr_value = float(snow) if pr_value == 0.0 else pr_value
        if not precip and code is not None and int(code) in ALERT_WEATHER_CODES_PRECIP:
            precip = True
        if precip:
            added += insert_alert_db(conn, location_id, ts, "precipitation",
                                     pr_value, f"Wykryto możliwe opady o {hour_str}")
    except Exception:
        LOGGER.exception("Błąd przy sprawdzaniu rekordu")
    return added


def analyze_payload_and_alert(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any]) -> int:
    """Analizuj payload i zapisuj alerty tylko na dziś + 2 dni."""
    added = 0
    try:
        now = datetime.utcnow()
        max_dt = now + timedelta(days=2)

        def is_in_range(ts: str) -> bool:
            try:
                t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return now <= t_dt <= max_dt
            except Exception:
                return False

        # hourly
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        if times:
            def arr(name): return hourly.get(name, [])
            temps, winds, rains, snows = arr("temperature_2m"), arr("wind_speed_10m"), arr("rain"), arr("snowfall")
            codes = arr("weather_code") or arr("weathercode")
            for i, ts in enumerate(times):
                if not is_in_range(ts):
                    continue
                added += _check_row_for_alerts(location_id, ts,
                                               temps[i] if i < len(temps) else None,
                                               winds[i] if i < len(winds) else None,
                                               rains[i] if i < len(rains) else None,
                                               snows[i] if i < len(snows) else None,
                                               codes[i] if i < len(codes) else None,
                                               conn)

        # minutely_15
        minutely = payload.get("minutely_15", {})
        times = minutely.get("time", [])
        if times:
            def arr2(name): return minutely.get(name, [])
            temps, winds, rains, snows = arr2("temperature_2m"), arr2("wind_speed_10m"), arr2("rain"), arr2("snowfall")
            codes = arr2("weather_code") or arr2("weathercode")
            for i, ts in enumerate(times):
                if not is_in_range(ts):
                    continue
                added += _check_row_for_alerts(location_id, ts,
                                               temps[i] if i < len(temps) else None,
                                               winds[i] if i < len(winds) else None,
                                               rains[i] if i < len(rains) else None,
                                               snows[i] if i < len(snows) else None,
                                               codes[i] if i < len(codes) else None,
                                               conn)
    except Exception:
        LOGGER.exception("Błąd podczas analizy payloadu")
    return added
